# Copyright (c) 2019 Aiven, Helsinki, Finland. https://aiven.io/
import contextlib
import logging
import os
import time

from pghoard.rohmu import get_transfer
from pghoard.rohmu.compressor import DecompressSink
from pghoard.rohmu.encryptor import DecryptSink


def download_binlog(config, queue_in, queue_out):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s")
    downloader = BinlogDownloader(config, queue_in, queue_out)
    downloader.loop()


class BinlogDownloader:
    def __init__(self, config, queue_in, queue_out):
        self.config = config
        self.log = logging.getLogger(self.__class__.__name__)
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.rsa_private_key_pem = config["rsa_private_key_pem"].encode("ascii")
        self.transfer = None

    def loop(self):
        while True:
            action = self.queue_in.get()
            if not action:
                return
            start_time = time.monotonic()
            exception = None
            try:
                self.log.info("Starting to download %r", action["remote_key"])
                if self.transfer is None:
                    self.transfer = get_transfer(self.config["object_storage"])
                # TODO: Monitor progress
                with contextlib.suppress(OSError):
                    os.remove(action["local_file_name"])
                with open(action["local_file_name"], "wb") as output_file:
                    output_obj = DecompressSink(output_file, action["compression_algorithm"])
                    output_obj = DecryptSink(output_obj, action["remote_file_size"], self.rsa_private_key_pem)
                    self.transfer.get_contents_to_fileobj(action["remote_key"], output_obj)
                    self.log.info(
                        "%r successfully saved as %r in %.2f seconds", action["remote_key"], action["local_file_name"],
                        time.monotonic() - start_time
                    )
            except Exception as ex:  # pylint: disable=broad-except
                exception = ex
                self.log.exception("An error occurred while handling action")

            # Convert exception to string as it might not be picklable
            result = {
                **action,
                "duration": time.monotonic() - start_time,
                "message": str(exception) if exception else None,
                "result": "failure" if exception else "success",
            }
            self.queue_out.put(result)
