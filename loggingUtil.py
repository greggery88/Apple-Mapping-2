import logging
import pandas as pd
import utilities


def setup_logging(
    label: str,
) -> logging.Logger:
    # create logger with 'spam_application'
    logger = logging.getLogger(label)
    logger.setLevel(logging.DEBUG)

    # create a console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    ch.setFormatter(CustomFormatter())

    logger.addHandler(ch)
    return logger


class LogInfo(logging.Logger):
    def __init__(
        self,
        logger_name: str,
        print_log_msg: bool = True,
        write_to_debug_file: bool = True,
    ) -> None:
        super().__init__(logger_name)
        self.logger: logging.Logger = setup_logging(logger_name)
        self.print_log_message: bool = print_log_msg
        self.write_to_debug_file: bool = write_to_debug_file


def _formated_log_str(
    name: str,
    info: str = None,
    fix: str = None,
    loc: str = None,
) -> str:
    """
    Generates a formatted string representing log information based on the provided
    parameters. The generated log string includes the name, info, fix, and location
    (if available).

    :param name: The name associated with the log entry.
    :type name: str
    :param info: The informational message for the log entry.
    :type info: str
    :param fix: The suggested fix or resolution related to the log entry.
    :type fix: str
    :param loc: The location or context of the log entry. If None, location will
                not be included in the formatted log entry.
    :type loc: str
    :return: A formatted log string containing the specified information.
    :rtype: str
    """
    if loc is not None:
        return (
            f": {name} >\n"
            f" - INFO: {info}\n"
            f" -  LOC: {loc}\n"
            f" -  FIX: {fix}\n"
            "   lINE: -------- "
        )
    else:
        return (
            f": {name} >\n"
            f" - INFO: {info}\n"
            f" -  FIX: {fix}\n"
            "   lINE: -------- "
        )


def log_formated(
    log_level, name: str, info: str, fix: str, to_file: bool = False, df=None
) -> None:
    if df is None:
        log_level(
            _formated_log_str(
                name,
                info,
                fix,
            )
        )
    else:
        if to_file:
            log_level(_formated_log_str(name, info, fix, "file -> debugInfo/" + name))
            utilities.to_csv_file(path="debugInfo", name=name, df=df)
        else:
            log_level(_formated_log_str(name, info, fix, None))


def log_formated_warning(
    log,
    warning_name: str = "No warning name!",
    warning_info: str = "no written log message implemented",
    warning_fix: str = "no written log message implemented",
):
    log_formated(
        log.warning,
        warning_name,
        warning_info,
        warning_fix,
    )


def log_formated_warning_with_df(
    log: logging.Logger,
    df: pd.DataFrame,
    warning_name: str = "No warning name!",
    warning_info: str = "no written log message implemented",
    warning_fix: str = "no written log message implemented",
    write_to_debug_file: bool = True,
) -> None:
    log_formated(
        log.warning,
        warning_name,
        warning_info,
        warning_fix,
        to_file=write_to_debug_file,
        df=df,
    )


def log_formated_error_with_df(
    log: logging.Logger,
    error_name: str,
    df: pd.DataFrame,
    error_info: str = "no written log message implemented",
    error_fix: str = "no written log message implemented",
    write_to_debug_file: bool = True,
) -> None:
    log_formated(
        log.error, error_name, error_info, error_fix, to_file=write_to_debug_file, df=df
    )


class CustomFormatter(logging.Formatter):

    blue = "\033[94m"
    grey = "\x1b[37m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    green = "\x1b[32m"
    format = "%(levelname)s - %(name)s - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.DEBUG: blue + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
