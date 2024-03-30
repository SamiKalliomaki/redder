use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    time::{Duration, Instant},
};

use monoio::io::{AsyncReadRent, AsyncWriteRent};

use crate::{
    database::Database,
    protocol::{RedisBufStream, RedisReadExt, RedisWrite},
};

struct ParsedArgs {
    args: Vec<String>,
    named_args: HashMap<&'static str, Vec<String>>,
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

fn create_command_specs<'db, Stream: AsyncReadRent + AsyncWriteRent>() -> CmdSpecs<'db, Stream> {
    let mut specs: CmdSpecs<'db, Stream> = HashMap::new();

    specs.insert(
        "ping",
        CmdListItem::Spec(CmdSpec::new(|conn, _| Box::pin(conn.handle_ping()))),
    );
    specs.insert(
        "echo",
        CmdListItem::Spec(
            CmdSpec::new(|conn, command| Box::pin(conn.handle_echo(command))).leading(1),
        ),
    );
    specs.insert(
        "get",
        CmdListItem::Spec(
            CmdSpec::new(|conn, command| Box::pin(conn.handle_get(command))).leading(1),
        ),
    );
    specs.insert(
        "set",
        CmdListItem::Spec(
            CmdSpec::new(|conn, command| Box::pin(conn.handle_set(command)))
                .leading(2)
                .named("px", 1),
        ),
    );

    return specs;
}

fn parse_args(
    leading_argc: usize,
    named_arg_argc: &HashMap<&'static str, usize>,
    mut unparsed_args: VecDeque<String>,
) -> anyhow::Result<ParsedArgs> {
    anyhow::ensure!(unparsed_args.len() >= leading_argc, "Not enough arguments");

    let mut args = unparsed_args.drain(..leading_argc).collect::<Vec<String>>();
    let mut named_args: HashMap<&'static str, Vec<String>> = HashMap::new();

    while !unparsed_args.is_empty() {
        let arg = unparsed_args.pop_front().unwrap().to_lowercase();

        let named_arg = named_arg_argc.get_key_value(arg.as_str());
        if let Some((key, argc)) = named_arg {
            if *argc > unparsed_args.len() {
                anyhow::bail!("Missing value for named argument: {}", arg);
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
    stream: RedisBufStream<Stream>,
}

impl<'db, Stream: AsyncReadRent + AsyncWriteRent> Connection<'db, Stream> {
    pub(crate) fn new(db: &'db Database, stream: Stream) -> Self {
        Self {
            specs: create_command_specs(),
            db,
            stream: RedisBufStream::new(stream),
        }
    }

    async fn handle_ping(&mut self) -> anyhow::Result<()> {
        self.stream.write_simple_string("PONG").await?;
        Ok(())
    }

    async fn handle_echo(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        self.stream
            .write_bulk_string(command.args[0].clone().into_bytes())
            .await?;
        Ok(())
    }

    async fn handle_get(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let key = &command.args[0];

        let value = self.db.get(key);
        match value {
            Some(value) => {
                self.stream.write_bulk_string(value.into_bytes()).await?;
            }
            None => {
                self.stream.write_null_bulk_string().await?;
            }
        }
        Ok(())
    }

    async fn handle_set(&mut self, command: ParsedArgs) -> anyhow::Result<()> {
        let mut args = command.args.into_iter();
        let key = args.next().unwrap();
        let value = args.next().unwrap();

        let expiry: Option<Instant>;
        if let Some(px) = command.named_args.get("px") {
            let px = px[0].parse::<u64>()?;
            expiry = Some(Instant::now() + Duration::from_millis(px));
        } else {
            expiry = None;
        }

        self.db.set(key, value, expiry);
        self.stream.write_simple_string("OK").await?;
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
                anyhow::ensure!(!command.is_empty(), "Unexpected end of command: {:?}", names);

                let name = command.pop_front().unwrap().to_lowercase();
                match map.get(name.as_str()) {
                    Some(CmdListItem::Spec(spec)) => {
                        found_spec = spec;
                        break;
                    },
                    Some(CmdListItem::SubSpecs(sub_cmds)) => {
                        names.push(name);
                        map = sub_cmds;
                    },
                    None => {
                        anyhow::bail!("Unknown command: {}", command[0]);
                    }
                };
            }

            let parsed_args = parse_args(found_spec.leading_argc, &found_spec.named_arg_argc, command)?;
            let handler = found_spec.handler;
            handler(self, parsed_args).await?;
        }
    }
}
