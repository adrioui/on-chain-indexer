.PHONY: benchmark test

benchmark:
	@echo "Running API benchmarks..."
	k6 run benchmarks/bench_api_watch.js
	k6 run benchmarks/bench_api_list.js
	@echo "\nRunning throughput benchmark..."
	@echo "Ensure Anvil is running: anvil --host 0.0.0.0 --block-time 1"
	python3 benchmarks/bench_throughput.py

test:
	pytest
