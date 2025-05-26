type LogicTime = int


def age(
    beginning: LogicTime | None, current_time: LogicTime | None
) -> LogicTime:
    if beginning is None or current_time is None:
        return 0

    return current_time - beginning + 1
