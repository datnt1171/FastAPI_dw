from jose import jwt, JWTError
from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import logging
from app.core.config import settings

# Configuration - adjust these according to your Django settings
JWT_SECRET_KEY = settings.JWT_SECRET_KEY
JWT_ALGORITHM = settings.JWT_ALGORITHM

security = HTTPBearer()
logger = logging.getLogger(__name__)


def decode_jwt_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        # Decode JWT token
        payload = jwt.decode(
            credentials.credentials,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM]
        )
        return payload
        
    except JWTError as e:
        logger.error(f"JWT decode error: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Given token not valid for any token type"
        )


def has_permission(required_permission: str):
    
    def check(payload: dict = Depends(decode_jwt_token)):
        # Admin bypass
        if payload.get('role') == 'admin':
            logger.info(f"Admin user {payload.get('username')} bypassing permission check")
            return True
        
        # Check permission
        permissions = payload.get('permissions', [])
        if required_permission not in permissions:
            logger.info(f"User {payload.get('username')} Required: {required_permission}")
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You do not have permission to perform this action."
            )
        
        return payload
        
    return check