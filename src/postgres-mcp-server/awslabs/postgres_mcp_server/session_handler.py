# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import logging
import time
from typing import Dict, Any, Optional

logger = logging.getLogger("postgresql-mcp-server")

class SessionHandler:
    def __init__(self, session_timeout=1800):
        self.sessions = {}
        self.session_locks = {}
        self.session_timeout = session_timeout  # in seconds
        self.cleanup_task = None
    
    async def start(self):
        """Start the session handler and its cleanup task"""
        self.cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())
        logger.info("Session handler started")
    
    async def stop(self):
        """Stop the session handler and its cleanup task"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Session handler stopped")
    
    def get_session_lock(self, session_id: str):
        """Get or create a lock for the given session"""
        if session_id not in self.session_locks:
            self.session_locks[session_id] = asyncio.Lock()
            self.sessions[session_id] = {
                "created_at": time.time(),
                "last_access": time.time(),
                "db_connection": None,
                "connection_params": {}
            }
            logger.info(f"New session registered: {session_id}")
        else:
            # Update last access time
            self.sessions[session_id]["last_access"] = time.time()
            
        return self.session_locks[session_id]
    
    def get_connection(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get the database connection for the given session"""
        if session_id in self.sessions:
            return self.sessions[session_id].get("db_connection")
        return None
    
    def set_connection(self, session_id: str, connection: Any, connection_params: Dict[str, Any]):
        """Set the database connection for the given session"""
        if session_id in self.sessions:
            self.sessions[session_id]["db_connection"] = connection
            self.sessions[session_id]["connection_params"] = connection_params
            logger.info(f"Database connection set for session: {session_id}")
    
    def get_connection_params(self, session_id: str) -> Dict[str, Any]:
        """Get the connection parameters for the given session"""
        if session_id in self.sessions:
            return self.sessions[session_id].get("connection_params", {})
        return {}
    
    def close_connection(self, session_id: str):
        """Close the database connection for the given session"""
        if session_id in self.sessions and self.sessions[session_id].get("db_connection"):
            connection = self.sessions[session_id]["db_connection"]
            if hasattr(connection, "disconnect") and callable(connection.disconnect):
                try:
                    connection.disconnect()
                    logger.info(f"Database connection closed for session: {session_id}")
                except Exception as e:
                    logger.error(f"Error closing database connection for session {session_id}: {str(e)}")
            self.sessions[session_id]["db_connection"] = None
    
    async def _cleanup_expired_sessions(self):
        """Periodically clean up expired sessions"""
        try:
            while True:
                await asyncio.sleep(60)  # Check every minute
                current_time = time.time()
                expired_sessions = [
                    sid for sid, session in self.sessions.items()
                    if current_time - session["last_access"] > self.session_timeout
                ]
                
                for sid in expired_sessions:
                    if sid in self.sessions:
                        # Close database connection if it exists
                        self.close_connection(sid)
                        del self.sessions[sid]
                    if sid in self.session_locks:
                        del self.session_locks[sid]
                    logger.info(f"Expired session removed: {sid}")
                
                if expired_sessions:
                    logger.info(f"Cleaned up {len(expired_sessions)} expired sessions")
                
                # Log active session count
                logger.info(f"Active sessions: {len(self.sessions)}")
        except asyncio.CancelledError:
            logger.info("Session cleanup task cancelled")
        except Exception as e:
            logger.error(f"Error in session cleanup: {str(e)}")

# Global session handler instance
session_handler = SessionHandler()
