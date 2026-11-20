import logging

# The fault-injection tests produce expected warnings (invalid messages,
# dropped connections); keep the test output readable.
logging.disable(logging.WARNING)
