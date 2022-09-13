class AgentIsNotExist(Exception):
    def __init__(self, agent, *args, **kwargs) -> None:
        message = f"Agent <{agent}> is not existed."
        super().__init__(message, *args, **kwargs)