test:
	CODECRAFTERS_SUBMISSION_DIR=~/source/byo/redis CODECRAFTERS_TEST_CASES_JSON=`sed -z 's/\[.*START/\[/' all_cases.json | sed -z 's/,\s*END.*]/\]/'` ../redis-tester/dist/main.out
