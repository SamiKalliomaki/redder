use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    time::Duration,
};

use bytes::BytesMut;
use monoio::{
    io::{AsyncReadRent, AsyncWriteRent},
    time::Instant,
};

use crate::{
    database::{Database, Value},
    protocol::{RedisReadExt, RedisWrite}, buf_reader::TcpBufReader,
};

struct ParsedArgs {
    args: Vec<BytesMut>,
    named_args: HashMap<&'static str, Vec<BytesMut>>,
}

type CmdResultFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + 'a>>;
type CmdHandler<'db, Stream> =
    for<'a> fn(&'a mut Connection<'db, Stream>, ParsedArgs) -> CmdResultFuture<'a>;

struct CmdSpec<'db, Stream: AsyncReadRent + AsyncWriteRent> {
    leading_argc: usize,
    named_arg_argc: HashMap<&'static str, usize>,
    handler: CmdHandler<'db, Stream>,
}

enum CmdListItem<'db, Stream: AsyncReadRent + AsyncWriteRent> {
    Spec(CmdSpec<'db, Stream>),
    SubSpecs(CmdSpecs<'db, Stream>),
}

type CmdSpecs<'db, Stream> = HashMap<&'static str, CmdListItem<'db, Stream>>;

impl<'db, Stream: AsyncReadRent + AsyncWriteRent> CmdSpec<'db, Stream> {
    fn new(handler: CmdHandler<'db, Stream>) -> Self {
        Self {
            leading_argc: 0,
            named_arg_argc: HashMap::new(),
            handler,
        }
    }

    fn leading(mut self, count: usize) -> Self {
        self.leading_argc = count;
        self
    }

    fn named(mut self, name: &'static str, count: usize) -> Self {
        self.named_arg_argc.insert(name, count);
        self
    }

    fn flag(mut self, name: &'static str) -> Self {
        self.named_arg_argc.insert(name, 0);
        self
    }
}

macro_rules! cmd {
    ($map:expr, $name:expr, $handler:ident $(, $mod_name:ident ( $( $mod_arg:expr ),* ) )* ) => {
        let spec = CmdSpec::new(|conn, command|
              Box::pin(conn.$handler(command))
        );
        $(
            let spec = spec.$mod_name($( $mod_arg, )*);
        )*
        $map.insert(
          $name,
          CmdListItem::Spec(spec));
    }
}

fn create_command_specs<'db, Stream: AsyncReadRent + AsyncWriteRent>() -> CmdSpecs<'db, Stream> {
    let mut specs: CmdSpecs<'db, Stream> = HashMap::new();

    cmd!(specs, "ping", handle_ping);
    cmd!(specs, "echo", handle_echo, leading(1));
    cmd!(specs, "get", handle_get, leading(1));
    cmd!(specs, "set", handle_set, leading(2), named("px", 1));
    cmd!(specs, "keys", handle_keys, leading(1));

    {
        // Subcommand: config
        let mut sub_specs: CmdSpecs<'db, Stream> = HashMap::new();
        cmd!(sub_specs, "get", handle_config_get, leading(1));
        specs.insert("config", CmdListItem::SubSpecs(sub_specs));
    }

    return specs;
}

fn parse_args(
    leading_argc: usize,
    named_arg_argc: &HashMap<&'static str, usize>,
    mut unparsed_args: VecDeque<BytesMut>,
) -> anyhow::Result<ParsedArgs> {
    anyhow::ensure!(unparsed_args.len() >= leading_argc, "Not enough arguments");

    let mut args = unparsed_args
        .drain(..leading_argc)
        .collect::<Vec<BytesMut>>();
    let mut named_args: HashMap<&'static str, Vec<BytesMut>> = HashMap::new();

    while !unparsed_args.is_empty() {
        let arg = unparsed_args.pop_front().unwrap();
        let lowercase = String::from_utf8_lossy(&arg).to_lowercase();

        let named_arg = named_arg_argc.get_key_value(lowercase.as_str());
        if let Some((key, argc)) = named_arg {
            if *argc > unparsed_args.len() {
                anyhow::bail!("Missing value for named argument: {}", lowercase);
            }
            named_args.insert(key, unparsed_args.drain(..*argc).collect());
        } else {
            args.push(arg);
        }
    }

    Ok(ParsedArgs { args, named_args })
}

pub(crate) struct Connection<'db, Stream: AsyncReadRent + AsyncWriteRent> {
    specs: CmdSpecs<'db, Stream>,
    db: &'db Database,
    stream: TcpBufReader<Stream>,
}

impl<'db, Stream: AsyncReadRent + AsyncWriteRent> Connection<'db, Stream> {
    pub(crate) fn new(db: &'db Database, stream: Stream) -> Self {
        Self {
            specs: create_command_specs(),
            db,
            stream: TcpBufReader::new(stream),
        }
    }

    async fn handle_ping(&mut self, _: ParsedArgs) -> anyhow::Result<()> {
        self.stream.write_simple_string("PONG").await?;
        Ok(())
    }

    async fn handle_echo(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let mut args = command.args.into_iter();
        let echo = args.next().unwrap();

        self.stream.write_bulk_string(echo).await?;
        Ok(())
    }

    async fn handle_get(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let mut args = command.args.into_iter();
        let key = args.next().unwrap();

        let value;
        {
            let lock = self.db.read(0);
            match lock.get(&key) {
                Some(Value::String(data)) => {
                    value = Some(data.clone());
                }
                // TODO: Handle more data types?
                _ => {
                    value = None;
                }
            }
        }

        self.stream.write_bulk_string_opt(value).await?;
        Ok(())
    }

    async fn handle_set(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let mut args = command.args.into_iter();
        let key = args.next().unwrap();
        let value = args.next().unwrap();

        let expiry: Option<Instant>;
        if let Some(px) = command.named_args.get("px") {
            let px = std::str::from_utf8(&px[0])?.parse::<u64>()?;
            expiry = Some(Instant::now() + Duration::from_millis(px));
        } else {
            expiry = None;
        }

        let key = key.to_vec().into_boxed_slice();
        let value = value.to_vec();
        {
            let mut lock = self.db.write(0);
            match expiry {
                Some(expiry) => lock.set_expiry(key.clone(), expiry),
                None => lock.unset_expiry(&key),
            }
            lock.set(key, Value::String(value));
        }
        self.stream.write_simple_string("OK").await?;
        Ok(())
    }

    async fn handle_config_get(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let mut args = command.args.into_iter();
        let key = args.next().unwrap();

        let value = self.db.get_config(&key);
        self.stream.write_array(2).await?;
        self.stream.write_bulk_string(key).await?;
        self.stream
            .write_bulk_string_opt(value.map(|v| v.into_bytes()))
            .await?;
        Ok(())
    }

    async fn handle_keys(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let mut args = command.args.into_iter();
        let pattern = args.next().unwrap();

        if pattern.as_ref() != b"*" {
            anyhow::bail!("Unsupported pattern: {:?}", pattern);
        }

        let keys;
        {
            let lock = self.db.read(0);
            keys = lock.all_keys().into_iter().map(|k| k.to_vec()).collect::<Vec<_>>();
        }
        self.stream.write_array(keys.len() as i64).await?;
        for key in keys {
            self.stream.write_bulk_string(key).await?;
        }
        Ok(())
    }

    pub(crate) async fn handle_connection(&mut self) -> anyhow::Result<()> {
        let mut names: Vec<String> = Vec::new();
        loop {
            let mut command: VecDeque<_> = self.stream.read_string_array().await?.into();
            names.clear();

            let mut map = &self.specs;
            let found_spec: &CmdSpec<'db, Stream>;
            loop {
                anyhow::ensure!(
                    !command.is_empty(),
                    "Unexpected end of command: {:?}",
                    names
                );

                let arg = command.pop_front().unwrap();
                let lowercase = String::from_utf8_lossy(&arg).to_lowercase();
                match map.get(lowercase.as_str()) {
                    Some(CmdListItem::Spec(spec)) => {
                        found_spec = spec;
                        break;
                    }
                    Some(CmdListItem::SubSpecs(sub_cmds)) => {
                        names.push(lowercase);
                        map = sub_cmds;
                    }
                    None => {
                        anyhow::bail!("Unknown command: {:?} -> {}", names, lowercase);
                    }
                };
            }

            let parsed_args =
                parse_args(found_spec.leading_argc, &found_spec.named_arg_argc, command)?;
            let handler = found_spec.handler;
            handler(self, parsed_args).await?;
        }
    }
}
