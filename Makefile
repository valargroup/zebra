.PHONY: help

include make/zcashd-compat.mk

help:
	@echo "Available targets:"
	@echo "  compat-zebrad-start-supervised   Start zebrad with zcashd supervision enabled"
	@echo "  compat-zebrad-start-unsupervised Start zebrad with zcashd supervision disabled"
	@echo "  compat-zcashd-start-standalone   Start zcashd -unity as a standalone process"
	@echo "  compat-zebrad-status             Check zebrad liveness and Zebra RPC health"
	@echo "  compat-zcashd-status             Check zcashd liveness and unity RPC health"
	@echo "  compat-status-sync               Run both status checks and enforce max drift"
