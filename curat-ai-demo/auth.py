"""Auth helpers for alation APIs."""

from datetime import datetime

import requests
import structlog
from pydantic import BaseModel

logger = structlog.get_logger()


class AccessTokenResponse(BaseModel):
    api_access_token: str
    user_id: int
    created_at: str
    token_expires_at: str
    token_status: str


class Auth:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session: requests.Session | None = None


class AlationAuth(Auth):
    """Alation authentication class.

    See https://developer.alation.com/dev/docs/authentication-into-alation-apis
    """

    BASE_URL = "https://master-uat-qause1.mtqa.alationcloud.com"
    REFRESH_TOKEN_URL = "/integration/v1/createRefreshToken/"
    ACCESS_TOKEN_URL = "/integration/v1/createAPIAccessToken/"

    def __init__(
        self,
        username: str,
        password: str,
        base_url: str | None = None,
        token_name: str = "AlationAPI",
    ):
        super().__init__(base_url or self.BASE_URL)
        self.username = username
        self.password = password
        self.token_name = token_name
        self.refresh_token = None
        self.user_id = None
        self.access_token_obj = None

    def get_refresh_token(self) -> tuple[str, int]:
        """
        Get a refresh token from Alation API.

        Returns:
            tuple[str, int]: A tuple containing the refresh token and user ID
        """
        data = {
            "username": self.username,
            "password": self.password,
            "name": self.token_name,
        }

        response = requests.post(self.base_url + self.REFRESH_TOKEN_URL, json=data)
        response.raise_for_status()

        res = response.json()
        self.refresh_token = res["refresh_token"]
        self.user_id = int(res["user_id"])

        return self.refresh_token, self.user_id

    def get_access_token(self, force_refresh: bool = False) -> str:
        """
        Get an access token using the refresh token. If no refresh token exists,
        it will be obtained first. If the token has expired, a new one will be requested.

        Args:
            force_refresh: Force refresh the token even if it hasn't expired

        Returns:
            str: The access token
        """
        # Get refresh token if we don't have one
        if self.refresh_token is None or self.user_id is None:
            self.get_refresh_token()

        # Check if token has expired or force refresh is requested
        if force_refresh or self._is_token_expired():
            data = {"refresh_token": self.refresh_token, "user_id": self.user_id}

            response = requests.post(self.base_url + self.ACCESS_TOKEN_URL, json=data)
            response.raise_for_status()

            res = response.json()
            self.access_token_obj = AccessTokenResponse.model_validate(res)

        return self.access_token_obj.api_access_token

    def _is_token_expired(self) -> bool:
        """
        Check if the current access token has expired.

        Returns:
            bool: True if token has expired or doesn't exist, False otherwise
        """
        if self.access_token_obj is None:
            return True

        # Parse the expiration time from the token response
        try:
            expiry_time = datetime.fromisoformat(
                self.access_token_obj.token_expires_at.replace("Z", "+00:00")
            )
            current_time = datetime.now().astimezone()
            return current_time >= expiry_time
        except (ValueError, AttributeError):
            logger.warning(
                "Failed to parse token expiration time.",
                token_expires_at=self.access_token_obj.token_expires_at,
            )
            # If we can't parse the expiration time, assume token is expired
            return True

    def get_auth_headers(self) -> dict[str, str]:
        """
        Get authentication headers with a valid access token.

        Returns:
            dict[str, str]: Headers dictionary with the TOKEN field set
        """
        token = self.get_access_token()
        return {"TOKEN": token}

    def get_authenticated_session(self) -> requests.Session:
        """
        Get a requests Session object with authentication headers set.

        Returns:
            requests.Session: Authenticated session object
        """
        session = requests.Session()
        session.headers.update(self.get_auth_headers())
        self.session = session
        return session


class NumbersStationAuth(Auth):
    """
    Authentication class for NumbersStation API.
    """

    BASE_URL = "https://api.numbersstation.ai"

    def __init__(self, username: str, password: str, base_url: str | None = None):
        """
        Initialize the NSAuth class with credentials.

        Args:
            username: NumbersStation username
            password: NumbersStation password
            base_url: Base URL for the API (default: "https://api.numbersstation.ai")
        """
        super().__init__(base_url or self.BASE_URL)
        self.username = username
        self.password = password

    def get_authenticated_session(self) -> requests.Session:
        """
        Get a requests Session object authenticated with NumbersStation credentials.

        Returns:
            requests.Session: Authenticated session object
        """

        # Create a new session
        session = requests.Session()

        # Prepare login data
        login_data = {
            "username": self.username,
            "password": self.password,
        }

        # Construct API base URL
        api_base_url = f"{self.base_url}/api/v1"

        # Perform login to set the cookie on the session automatically
        response = session.post(api_base_url + "/login/session", data=login_data)

        # Check if login was successful
        response.raise_for_status()

        # Verify the session works by fetching user info
        session.get(f"{api_base_url}/users/me")

        self.session = session
        return session


def get_alation_authenticated_session(
    username: str, password: str, token_name: str = "AlationAPI"
) -> requests.Session:
    """
    Utility function to quickly get an authenticated session.

    Args:
        username: Alation username
        password: Alation password
        token_name: Name for the token (default: "AlationAPI")

    Returns:
        requests.Session: Authenticated session object
    """
    auth = AlationAuth(username=username, password=password, token_name=token_name)
    return auth.get_authenticated_session()


def get_ns_authenticated_session(
    username: str, password: str, base_url: str | None = None
) -> requests.Session:
    """
    Utility function to quickly get an authenticated session for NumbersStation API.

    Args:
        username: NumbersStation username
        password: NumbersStation password
        base_url: Base URL for the API (default: "https://api.numbersstation.ai")

    Returns:
        requests.Session: Authenticated session object
    """
    auth = NumbersStationAuth(username=username, password=password, base_url=base_url)
    return auth.get_authenticated_session()
