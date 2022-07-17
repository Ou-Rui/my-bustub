#!/bin/bash
#
# example:  /bin/bash ./test-many.sh 500 grading_buffer_pool_manager_concurrency_test
# /bin/bash ./test-many.sh 1 grading_b_plus_tree_checkpoint_1_test
# /bin/bash ./test-many.sh 1 grading_b_plus_tree_checkpoint_2_sequential_test
# TODO: can't be parallel for now..., i.e. parallelism == "1"

rm -rf ./test-log
mkdir "test-log"

if [ $# -eq 1 ] && [ "$1" = "--help" ]; then
	echo "Usage: $0 [RUNS=100] [TEST='']"
	exit 1
fi

# Default to 100 runs unless otherwise specified
runs=100
if [ $# -gt 0 ]; then
	runs="$1"
fi

# Default to one tester per CPU unless otherwise specified
#parallelism=$(grep -c processor /proc/cpuinfo)
#if [ $# -gt 1 ]; then
#	parallelism="1"
#fi
parallelism="1"

# Default to no test filtering unless otherwise specified
test=""
if [ $# -gt 1 ]; then
	test="$2"
fi

# make
if ! make ${test}; then
  echo -e "\e[1;31mERROR: Build failed\e[0m"
  exit 1
fi

# Figure out where we left off
logs=$(find ./test-log -maxdepth 1 -name 'test-*.log' -type f -printf '.' | wc -c)
failed=$(grep -E 'FAILED TEST$' ./test-log/test-*.log | wc -l)
((success = logs - failed))
#success=$(grep -E '^PASS$' ./test-log/test-*.log | wc -l)
#((failed = logs - success))

# Finish checks the exit status of the tester with the given PID, updates the
# success/failed counters appropriately, and prints a pretty message.
finish() {
	if ! wait "$1"; then
		if command -v notify-send >/dev/null 2>&1 &&((failed == 0)); then
			notify-send -i weather-storm "Tests started failing" \
			    "failed"
#				"$(pwd)\n$(grep FAIL: -- ./test-log/*.log | sed -e 's/.*FAIL: / - /' -e 's/ (.*)//' | sort -u)"
		fi
		((failed += 1))
	else
		((success += 1))
	fi

	if [ "$failed" -eq 0 ]; then
		printf "\e[1;32m";
	else
		printf "\e[1;31m";
	fi

	printf "Done %03d/%d; %d ok, %d failed\n\e[0m" \
		$((success+failed)) \
		"$runs" \
		"$success" \
		"$failed"
}

waits=() # which tester PIDs are we waiting on?
is=()    # and which iteration does each one correspond to?

# Cleanup is called when the process is killed.
# It kills any remaining tests and removes their output files before exiting.
cleanup() {
	for pid in "${waits[@]}"; do
		kill "$pid"
		wait "$pid"
#		rm -rf "./test-log/test-${is[0]}.err" "./test-log/test-${is[0]}.log"
		is=("${is[@]:1}")
	done
	exit 0
}
trap cleanup SIGHUP SIGINT SIGTERM

# Run remaining iterations (we may already have run some)
for i in $(seq "$((success+failed+1))" "$runs"); do
	# If we have already spawned the max # of testers, wait for one to
	# finish. We'll wait for the oldest one beause it's easy.
	if [[ ${#waits[@]} -eq "$parallelism" ]]; then
		finish "${waits[0]}"
		waits=("${waits[@]:1}") # this funky syntax removes the first
		is=("${is[@]:1}")       # element from the array
	fi

	# Store this tester's iteration index
	# It's important that this happens before appending to waits(),
	# otherwise we could get an out-of-bounds in cleanup()
	is=("${is[@]}" $i)

	# Run the tester, passing -test.run if necessary
	./test/${test} > ./test-log/test-${i}.log &
	pid=$!
#	if [[ -z "$test" ]]; then
#		./tester -test.v 2> "./test-log/test-${i}.err" > "./test-log/test-${i}.log" &
#		pid=$!
#	else
#		./tester -test.run "$test" -test.v 2> "./test-log/test-${i}.err" > "./test-log/test-${i}.log" &
#		pid=$!
#	fi

	# Remember the tester's PID so we can wait on it later
	waits=("${waits[@]}" $pid)
done

# Wait for remaining testers
for pid in "${waits[@]}"; do
	finish "$pid"
done

if ((failed>0)); then
	exit 1
fi
exit 0