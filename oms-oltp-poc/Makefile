.PHONY: test run demo

test:
	python -m pytest

run:
	uvicorn oms_oltp.api:app --host 127.0.0.1 --port 8030

demo:
	python -m oms_oltp.demo
