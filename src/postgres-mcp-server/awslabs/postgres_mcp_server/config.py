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

import logging
import asyncio
import time
from contextlib import asynccontextmanager

class SessionHandler:
    def __init__(self, session_timeout=1800):
        self.sessions = {}
        self.session_locks = {}
        self.session_timeout = session_timeout  # in seconds
        self.cleanup_task = None
    
    async def start(self):
        """Start the session handler and its cleanup task"""
        self.cleanup_task = asyncio.create_task(self._cleanup_expired_sessions())
        logging.info("Session handler started")
    
    async def stop(self):
        """Stop the session handler and its cleanup task"""
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        logging.info("Session handler stopped")
    
    def get_session_lock(self, session_id: str):
        """Get or create a lock for the given session"""
        if session_id not in self.session_locks:
            self.session_locks[session_id] = asyncio.Lock()
            self.sessions[session_id] = {
                "created_at": time.time(),
                "last_access": time.time()
            }
            logging.info(f"New session registered: {session_id}")
        else:
            # Update last access time
            self.sessions[session_id]["last_access"] = time.time()
            
        return self.session_locks[session_id]
    
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
                        del self.sessions[sid]
                    if sid in self.session_locks:
                        del self.session_locks[sid]
                    logging.info(f"Expired session removed: {sid}")
                
                if expired_sessions:
                    logging.info(f"Cleaned up {len(expired_sessions)} expired sessions")
                
                # Log active session count
                logging.info(f"Active sessions: {len(self.sessions)}")
        except asyncio.CancelledError:
            logging.info("Session cleanup task cancelled")
        except Exception as e:
            logging.error(f"Error in session cleanup: {str(e)}")

def configure_logging():
    """Configure logging for the application"""
    logger = logging.getLogger("postgresql-mcp-server")
    logger.setLevel(logging.INFO)
    
    # Create console handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    
    # Add handler to logger
    logger.addHandler(handler)
    
    return logger

# Create a session handler instance
session_handler = SessionHandler()

@asynccontextmanager
async def server_lifespan(app):
    """
    Lifespan context manager for the FastMCP server.
    This runs when the server starts and stops.
    """
    # Server startup
    logger = logging.getLogger("postgresql-mcp-server")
    logger.info("Starting PostgreSQL MCP server")
    
    # Start the session handler
    await session_handler.start()
    
    yield
    
    # Server shutdown
    logger.info("Shutting down PostgreSQL MCP server")
    
    # Stop the session handler
    await session_handler.stop()
