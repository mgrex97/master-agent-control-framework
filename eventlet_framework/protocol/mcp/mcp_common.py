from struct import calcsize

# Machine Service Header
MCP_HEADER_PACK_STR = '!HII'
MCP_HEADER_SIZE = 10

assert calcsize(MCP_HEADER_PACK_STR) == MCP_HEADER_SIZE
