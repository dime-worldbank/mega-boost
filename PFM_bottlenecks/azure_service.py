from abc import ABC
from openai import OpenAI
from openai import AzureOpenAI
import instructor
from azure.identity import get_bearer_token_provider, ClientSecretCredential

# client set-up
credential = ClientSecretCredential(
       tenant_id = dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Tenant-ID"),
       client_id = dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Client-ID"),
       client_secret = dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Client-Secret")
   )
# set up Azure AD token provider
token_provider = get_bearer_token_provider(credential, dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Token-Cred"))
   
#dbutils.secrets.get(scope="DAPGPTKEYVAULT", key="GPT-APIM-Token-Cred"
# initialize AzureOpenAI client
client = AzureOpenAI(
    azure_endpoint="https://azapim.worldbank.org/conversationalai/v2/",
    azure_ad_token_provider=token_provider,
    api_version="2024-08-01-preview"
)

class BaseService(ABC):
    """
    Base class for all services. Automatically registers subclasses with the ServiceRegistry.
    """

    def __init_subclass__(cls, **kwargs):
        """
        Automatically registers subclasses with the ServiceRegistry when they are defined.
        """
        super().__init_subclass__(**kwargs)

        if not hasattr(cls, "service_name") or not isinstance(cls.service_name, str):
            raise AttributeError(
                f"Service class '{cls.__name__}' must define a string 'service_name'."
            )


class AzureOpenAIService(BaseService):
    """
    A service for interacting with Azure OpenAI using Instructor.
    """

    service_name = "azureopenai"
    
    DEFAULT_SYSTEM_PROMPT = '''You are a public finance expert working with a multilateral development institution...

    (same as before)
    '''

    def __init__(
        self,
        client: AzureOpenAI,
        system_prompt: Optional[str] = None,
    ):
        """
        Initializes the AzureOpenAIService with an existing AzureOpenAI client.
        """
        self.SYSTEM_PROMPT = system_prompt or self.DEFAULT_SYSTEM_PROMPT

        # Wrap your AzureOpenAI client using instructor
        self.client = instructor.from_openai(
            client,
            mode=instructor.Mode.JSON,
        )

    def execute(
        self,
        prompt: str,
        model: str,
        response_model: Type,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
        system_message: Optional[str] = None,
        **kwargs,
    ):
        """
        Executes a structured chat completion.
        """
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("The 'prompt' must be a non-empty string.")
        if not isinstance(model, str):
            raise ValueError("The 'model' must be a string.")

        system_message = system_message or self.SYSTEM_PROMPT

        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt},
        ]

        try:
            response = self.client.chat.completions.create(
                model=model,
                response_model=response_model,
                messages=messages,
                temperature=temperature,
                max_tokens=max_tokens,
                **kwargs,
            )
            return response

        except Exception as e:
            raise Exception(f"Error in AzureOpenAI API call: {e}")


service = AzureOpenAIService(client=client)