import unittest
import utilities
import loggingUtil
import collectDataFromSheets
import logging


class MyTestCase(unittest.TestCase):
    def test_logger_setup(self):
        test_parent_logger = loggingUtil.setup_logging("test_parent_logger")
        child_logger = test_parent_logger.getChild("test_logger")
        child_logger.info("fseafq")
        child_logger.debug("fseafq")
        c2 = test_parent_logger.getChild("c2")
        c2.info("fseafq")
        c3 = test_parent_logger.getChild("c3")
        c3.setLevel(logging.DEBUG)
        c3.info("fseafq")
        c2.debug("fq")
        c3.critical("fseafq")
        print(c3.getEffectiveLevel())

        def log_func(log):
            log("faewfeafafafa")

        log_func(c3.debug)

        self.assertEqual(True, True)  # add assertion here

    def test_collect_data_from_sheet(self):
        collectDataFromSheets.collect_data_from_google_sheets_main(
            loggingUtil.setup_logging("testGCDLog")
        )
        self.assertEqual(True, True)


if __name__ == "__main__":
    unittest.main()
