### Example for converting from UNIX epoch timestamp to datetime or string and how to assign/change the timezone

import datetime as dt
import pytz

unix_epoch_timestamp = 1641480096

# to datetime format
dt.datetime.fromtimestamp(unix_epoch_timestamp, tz=dt.timezone.utc)

# to string format
dt.datetime.fromtimestamp(unix_epoch_timestamp, tz=dt.timezone.utc).strftime("%Y-%m-%d %T.%f")

# change/assign a timezone
dt.datetime.fromtimestamp(unix_epoch_timestamp, tz=dt.timezone.utc).astimezone(pytz.timezone('US/Eastern'))
