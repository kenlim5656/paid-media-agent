from simple_salesforce import Salesforce
from config import settings

_sf: Salesforce | None = None


def get_client() -> Salesforce:
    global _sf
    if _sf is None:
        _sf = Salesforce(
            username=settings.sf_username,
            password=settings.sf_password,
            security_token=settings.sf_security_token,
            domain=settings.sf_domain,
        )
    return _sf


def query(soql: str) -> list[dict]:
    sf = get_client()
    result = sf.query_all(soql)
    return result["records"]


def get_leads_missing_media_fields(since_hours: int = 1) -> list[dict]:
    soql = f"""
        SELECT Id, Email, CreatedDate, gclid__c, ga_client_id__c, utm_source__c
        FROM Lead
        WHERE CreatedDate = LAST_N_HOURS:{since_hours}
          AND (gclid__c = null OR ga_client_id__c = null)
    """
    return query(soql)


def get_accounts_with_open_opportunities() -> list[dict]:
    soql = """
        SELECT AccountId, Account.Website, Account.BillingState
        FROM Opportunity
        WHERE StageName NOT IN ('Closed Won', 'Closed Lost')
          AND IsClosed = false
    """
    return query(soql)
