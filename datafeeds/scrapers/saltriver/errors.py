class MeterNotFoundError(Exception):
    @classmethod
    def for_meter(cls, meter_id, channel_id):
        return cls(
            "No meter found for meter-id='{}', channel='{}'.".format(
                meter_id, channel_id
            )
        )

    @classmethod
    def for_account(cls, account_id):
        return cls("No meter found for account-id='{}'.".format(account_id))


class AmbiguousMeterError(Exception):
    @classmethod
    def for_meter(cls, meter_id, channel_id):
        super().__init__(
            "Multiple meters found with meter-id='{}', channel='{}'".format(
                meter_id, channel_id
            )
        )


class IntervalDataParseError(Exception):
    pass
