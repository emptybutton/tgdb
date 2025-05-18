type LogicTime = int


def age(beginning: LogicTime | None, current_time: LogicTime) -> LogicTime:
    if beginning is None:
        return 0

    return current_time - beginning + 1
