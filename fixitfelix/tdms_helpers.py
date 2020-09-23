import nptdms


def get_maximum_array_size(tdms_operator: nptdms.TdmsFile) -> int:
    """Returns the maximal array length saved in the TDMS file.

    Arguments:
    tdms_operator: Operator of the tdms file

    Returns:
    Maximal array length.
    """
    if len(tdms_operator.groups()) == 0:
        return 0
    return max(
        max(len(channel) for channel in group.channels())
        for group in tdms_operator.groups()
        if len(group.channels()) > 0
    )
