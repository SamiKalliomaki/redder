test:
	CODECRAFTERS_SUBMISSION_DIR=`pwd` CODECRAFTERS_TEST_CASES_JSON=`sed -z 's/\[.*START/\[/' all_cases.json | sed -z 's/,\s*END.*]/\]/'` ../redis-tester/dist/main.out
