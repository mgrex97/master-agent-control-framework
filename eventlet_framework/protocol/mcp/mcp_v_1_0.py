MAX_XID = 0xffffffff

MCP_HEADER_PACK_STR = '!HHII'
MCP_HEADER_SIZE = 12

# enum mcp_type
MCP_HELLO = 0
MCP_ERROR = 1
MCP_GET_CONFIG_REQUEST = 2
MCP_GET_CONFIG_REPLY = 3
MCP_SET_CONFIG = 4
MCP_EXECUTE_COMMAND_REQUEST = 5
MCP_EXECUTE_COMMAND_REPLY = 5
MCP_SEND_TSHARK_COMMAND = 10
MCP_REPLY_TSHARK_COMMAND = 11

# mcp execute command
MCP_EXECUTE_COMMAND_REQUEST_SIZE = 8
MCP_EXECUTE_COMMAND_REQUEST_STR = "!II"
