.PHONY: demo test monitor

demo:
	python -m data_governance.demo

monitor:
	python -m data_governance.monitor --db ../oms-oltp-poc/data/oms_oltp.sqlite --contract contracts/oms_event_contract.json

test:
	python -m pytest