import logging
import time
from datetime import datetime


# Custom formatter to include milliseconds and timezone
class TimeFormatter(logging.Formatter):

    utc_offset = time.strftime('%z')
    utc_offset_display = f"UTC{utc_offset[:3]}"  # Slice to display 'UTC+X' or 'UTC-X'

    def formatTime(self, record, datefmt=None):
        current_time = datetime.fromtimestamp(record.created)
        formatted_time = current_time.strftime('%d-%m-%Y %H:%M:%S.') + f'{current_time.microsecond // 1000:03d} ' + TimeFormatter.utc_offset_display
        return formatted_time
