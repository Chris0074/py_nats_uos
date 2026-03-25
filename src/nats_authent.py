from dataclasses import dataclass
import httpx

@dataclass
class OAuthCredentials:
    nats_host: str
    client_name: str
    client_id: str
    client_secret: str
    scope: str

    def __post_init__(self):
        self.token_endpoint = f"https://{self.nats_host}/oauth2/token"

    async def request_token(self) -> str:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(
                self.token_endpoint,
                headers={"Accept": "application/json"},
                auth=(self.client_id, self.client_secret),
                data={
                    "grant_type": "client_credentials",
                    "scope": self.scope,
                },
                timeout=10.0,
            )
            response.raise_for_status()
            data = response.json()
            #print("TOKEN RESPONSE:", data)
            token = data.get("access_token")
            if not token:
                raise ValueError(f"Response does not contain access_token: {data}")
            return token
