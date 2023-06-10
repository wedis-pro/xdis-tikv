MOCK_SOURCE_DIR ?= ./driver
MOCK_DESTINATION_DIR ?= ./driver/mocks

.PHONY: domain-mockery
domain-mockery:
	@rm -rf $(MOCK_DESTINATION_DIR)
	@go install github.com/vektra/mockery/v2@latest
	@mockery --all --case underscore --dir $(MOCK_SOURCE_DIR) --output $(MOCK_DESTINATION_DIR)