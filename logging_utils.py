# Importing the necessary Libraries
import logging
import logging.config
import logging.handlers

from inspect import getframeinfo, stack


def sanitize_msg(msg):
    try:
        msg = ascii(msg)
    except:
        pass
    return msg


class Logging(logging.Logger):
    def __init__(self, name="common_utils", session_id="" , **kwargs):
        super().__init__(name)
        self.log_levels = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
        new_conf = "/opt/python/lib/python3.9/site-packages/common_utils/logging.conf"
        self.load_logging_conf(new_conf)

        self.extra = {"session_id": session_id, "service_name": name}
        self.set_ids(**kwargs)

    def load_logging_conf(self, file_path):
        logging.config.fileConfig(file_path)
        logging.getLogger().setLevel(self.log_levels["info"])

    def set_ids(self, **kwargs):
        line_no = None
        file_name = None
        try:
            caller = getframeinfo(stack()[2][0])
            file_name = caller.filename
            line_no = caller.lineno
            current_func_name = caller.function
        except Exception as e:
            message = "Failed to get caller stack"
            logging.error("########", message, e)

        self.line_no = line_no
        self.file_name = file_name
        self.current_func_name = current_func_name
        temp = {
            "fileName": self.file_name,
            "lineNo": self.line_no,
            "currentFuncName": self.current_func_name,
        }
        self.extra.update(temp)

    def basicConfig(self, *args, **kwargs):
        logging.basicConfig(**kwargs)

    def debug(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.debug(msg, extra=self.extra, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.info(msg, extra=self.extra, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.warning(msg, extra=self.extra, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.error(msg, extra=self.extra, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.critical(msg, extra=self.extra, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.set_ids()
        msg = sanitize_msg(msg)
        logging.exception(msg, extra=self.extra, *args, **kwargs)

    def getLogger(self, name=None):
        return logging.getLogger(name=name)

    def disable(self, level):
        logging.disable(level)
