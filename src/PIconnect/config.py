from tzlocal import get_localzone_name
class PIConfigContainer:
    _default_timezone = get_localzone_name()

    @property
    def DEFAULT_TIMEZONE(self):
        return self._default_timezone

    @DEFAULT_TIMEZONE.setter
    def DEFAULT_TIMEZONE(self, value):
        import pytz

        if value not in pytz.all_timezones:
            raise ValueError(
                "{v!r} not found in pytz.all_timezones".format(v=value)
            )
        self._default_timezone = value


PIConfig = PIConfigContainer()
