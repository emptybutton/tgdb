type LogicTime = int


def age(current_time: LogicTime, beginning: LogicTime | None) -> LogicTime:
    if beginning is None:
        return 0

    return current_time - beginning + 1
