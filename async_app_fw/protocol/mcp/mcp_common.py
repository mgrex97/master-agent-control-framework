from struct import calcsize

# Machine Service Header
MCP_HEADER_PACK_STR = '!HIBI'
MCP_HEADER_SIZE = 11

assert calcsize(MCP_HEADER_PACK_STR) == MCP_HEADER_SIZE
