import logging
import os

import cluster_doctor.main as main


def _owned_file_handlers():
    root_logger = logging.getLogger()
    return [
        h
        for h in root_logger.handlers
        if isinstance(h, logging.FileHandler)
        and os.path.basename(h.baseFilename) == "app.log"
    ]


def test_root_logger_has_app_log_file_handler():
    file_handlers = _owned_file_handlers()
    assert len(file_handlers) == 1
    assert os.path.basename(file_handlers[0].baseFilename) == "app.log"


def test_application_log_message_reaches_log_file():
    file_handlers = _owned_file_handlers()
    assert file_handlers, "expected a FileHandler targeting app.log on the root logger"
    file_handler = file_handlers[0]

    marker = "unique marker: test_application_log_message_reaches_log_file"
    logger = logging.getLogger("cluster_doctor.some.module")
    logger.info(marker)
    file_handler.flush()

    with open(file_handler.baseFilename, encoding="utf-8") as f:
        contents = f.read()

    assert marker in contents


def test_configure_logging_is_idempotent():
    before = len(_owned_file_handlers())

    main.configure_logging()
    main.configure_logging()

    after = len(_owned_file_handlers())
    assert after == before
