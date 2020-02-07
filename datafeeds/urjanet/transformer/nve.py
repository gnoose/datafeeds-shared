from datafeeds.urjanet.transformer import (
    GenericBillingPeriod,
    UrjanetGridiumTransformer,
)
from datafeeds.urjanet.model import Account


class NVEnergyTransformer(UrjanetGridiumTransformer):
    def get_peak_demand(self) -> Optional[Decimal]:
        """Attempt to determine peak demand from the set of usage entities associated with a billing period.

        This is not very straightforward for NVEnergy; demand charges from bills show up in a variety of configurations.
        The main confounding issue with this utility is facility charges. A facility charge on a given statement is
        based on the maximum demand over some trailing period of months (so that if you have a heavy peak one month, you
        will be penalized for several months into the future). This is distinct from the demand charge on a given bill.
        However, Urjanet is not very consistent when it comes to representing facility charges, and in many cases they
        are indistinguishable from demand charges. Thus there is some risk here of representing the facility charge as
        the demand charge. We try to filter out facility charges based on some simple heuristics, but this is not
        guaranteed to succeed for all bills.
        """

        # Collect demand measurements from the set of usages, attempting to filter out facility charges
        # In recent history, Urjanet has been labelling facility charges with "FAC" in the UsageActualName field.
        candidate_demand_usages = [
            u for u in usages
            if u.MeasurementType == 'demand' and u.EnergyUnit == 'kW' and not 'fac' in usage.UsageActualName.lower()
        ]

        if candidate_demand_usages:
            return max([x.UsageAmount for x in candidate_demand_usages])

        # Note: this function returns 0 when no demand peak is found, opposed to 'None'.
        # This is a post-condition on the parent method. Otherwise, we fail in production
        # when posting to webapps.
        return 0
