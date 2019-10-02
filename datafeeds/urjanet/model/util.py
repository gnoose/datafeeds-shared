from . import Charge, Usage, Meter


def log_charge(logger, charge: Charge, indent: int = 0):
    """Helper function for logging basic information about an Urjanet Charge object"""
    indent_str = "\t" * indent
    logger.debug("{0}Name='{1}',Amt=${2},Start='{3}',End='{4}',PK={5}".format(
        indent_str,
        charge.ChargeActualName,
        charge.ChargeAmount,
        charge.IntervalStart,
        charge.IntervalEnd,
        charge.PK))


def log_usage(logger, usage: Usage, indent: int = 0) -> None:
    """Helper function for logging basic information about an Urjanet Usage object"""
    indent_str = "\t" * indent
    logger.debug("{0}Amt={1}{2},Start='{3}',End='{4}',PK={5}".format(
        indent_str,
        usage.UsageAmount,
        usage.EnergyUnit,
        usage.IntervalStart,
        usage.IntervalEnd,
        usage.PK))


def log_meter(logger, meter: Meter, indent: int = 0) -> None:
    """Helper function for logging basic information about an Urjanet Meter object"""
    indent_str = "\t" * indent
    logger.debug("{0}MeterNumber={1},PODid={2},ServiceType={3},Start='{4}',End='{5}',PK={6}".format(
        indent_str,
        meter.MeterNumber,
        meter.PODid,
        meter.ServiceType,
        meter.IntervalStart,
        meter.IntervalEnd,
        meter.PK))
