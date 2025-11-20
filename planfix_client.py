from planfix_api import PlanfixAPIClient

# Глобальный экземпляр клиента Planfix, используемый по всему приложению.
planfix_client = PlanfixAPIClient()

__all__ = ["planfix_client"]

