# Methods to append to database.py

    async def save_population_snapshot(self, channel: str, domain: str, connected_count: int, chat_count: int) -> None:
        """Save population snapshot and check for water marks."""
        def _save():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            timestamp = datetime.now(UTC).isoformat()
            
            # Save snapshot
            cursor.execute("""
                INSERT INTO population_snapshots (channel, domain, timestamp, connected_count, chat_count)
                VALUES (?, ?, ?, ?, ?)
            """, (channel, domain, timestamp, connected_count, chat_count))
            
            # Check for high water mark (last 24 hours)
            cursor.execute("""
                SELECT MAX(connected_count) as max_count FROM population_snapshots
                WHERE channel = ? AND domain = ?
                AND datetime(timestamp) >= datetime('now', '-1 day')
            """, (channel, domain))
            result = cursor.fetchone()
            max_count = result[0] if result else 0
            
            if connected_count >= max_count:
                # New high water mark
                cursor.execute("""
                    INSERT OR REPLACE INTO population_watermarks 
                    (channel, domain, timestamp, total_users, chat_users, is_high_mark)
                    VALUES (?, ?, ?, ?, ?, 1)
                """, (channel, domain, timestamp, connected_count, chat_count))
            
            # Check for low water mark (last 24 hours)
            cursor.execute("""
                SELECT MIN(connected_count) as min_count FROM population_snapshots
                WHERE channel = ? AND domain = ?
                AND datetime(timestamp) >= datetime('now', '-1 day')
            """, (channel, domain))
            result = cursor.fetchone()
            min_count = result[0] if result else float('inf')
            
            if connected_count <= min_count:
                # New low water mark
                cursor.execute("""
                    INSERT OR REPLACE INTO population_watermarks 
                    (channel, domain, timestamp, total_users, chat_users, is_high_mark)
                    VALUES (?, ?, ?, ?, ?, 0)
                """, (channel, domain, timestamp, connected_count, chat_count))
            
            conn.commit()
            conn.close()
            
        await asyncio.get_event_loop().run_in_executor(None, _save)
    
    async def get_water_marks(self, channel: str, domain: str, days: int = None) -> Dict[str, Any]:
        """Get high and low water marks for user population.
        
        Args:
            channel: Channel name
            domain: Domain name  
            days: Number of days to look back (None for all time)
            
        Returns:
            Dict with 'high' and 'low' water mark data
        """
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            date_filter = ""
            params = [channel, domain]
            if days is not None:
                date_filter = "AND datetime(timestamp) >= datetime('now', '-' || ? || ' days')"
                params.append(days)
            
            # Get high water mark
            cursor.execute(f"""
                SELECT timestamp, total_users, chat_users FROM population_watermarks
                WHERE channel = ? AND domain = ? AND is_high_mark = 1
                {date_filter}
                ORDER BY total_users DESC, timestamp DESC
                LIMIT 1
            """, params)
            high_mark = cursor.fetchone()
            
            # Get low water mark
            cursor.execute(f"""
                SELECT timestamp, total_users, chat_users FROM population_watermarks
                WHERE channel = ? AND domain = ? AND is_high_mark = 0
                {date_filter}
                ORDER BY total_users ASC, timestamp DESC
                LIMIT 1
            """, params)
            low_mark = cursor.fetchone()
            
            conn.close()
            
            return {
                'high': dict(high_mark) if high_mark else None,
                'low': dict(low_mark) if low_mark else None
            }
            
        return await asyncio.get_event_loop().run_in_executor(None, _get)
    
    async def record_movie_vote(self, channel: str, domain: str, media_title: str, 
                                media_type: str, media_id: str, username: str, vote: int) -> None:
        """Record a movie vote (1 for upvote, -1 for downvote)."""
        def _record():
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            timestamp = datetime.now(UTC).isoformat()
            
            cursor.execute("""
                INSERT OR REPLACE INTO movie_votes 
                (channel, domain, media_title, media_type, media_id, username, vote, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (channel, domain, media_title, media_type, media_id, username, vote, timestamp))
            
            conn.commit()
            conn.close()
            
        await asyncio.get_event_loop().run_in_executor(None, _record)
    
    async def get_movie_votes(self, channel: str, domain: str, media_title: str = None) -> Dict[str, Any]:
        """Get movie voting statistics.
        
        Args:
            channel: Channel name
            domain: Domain name
            media_title: Specific movie title (None for all movies)
            
        Returns:
            Dict with vote statistics or list of movies
        """
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if media_title:
                # Get votes for specific movie
                cursor.execute("""
                    SELECT 
                        media_title,
                        media_type,
                        media_id,
                        SUM(CASE WHEN vote > 0 THEN 1 ELSE 0 END) as upvotes,
                        SUM(CASE WHEN vote < 0 THEN 1 ELSE 0 END) as downvotes,
                        COUNT(*) as total_votes,
                        SUM(vote) as score
                    FROM movie_votes
                    WHERE channel = ? AND domain = ? AND media_title = ?
                    GROUP BY media_title, media_type, media_id
                """, (channel, domain, media_title))
                result = cursor.fetchone()
                conn.close()
                return dict(result) if result else None
            else:
                # Get all movies with votes
                cursor.execute("""
                    SELECT 
                        media_title,
                        media_type,
                        media_id,
                        SUM(CASE WHEN vote > 0 THEN 1 ELSE 0 END) as upvotes,
                        SUM(CASE WHEN vote < 0 THEN 1 ELSE 0 END) as downvotes,
                        COUNT(*) as total_votes,
                        SUM(vote) as score,
                        MAX(timestamp) as last_vote
                    FROM movie_votes
                    WHERE channel = ? AND domain = ?
                    GROUP BY media_title, media_type, media_id
                    ORDER BY score DESC
                """, (channel, domain))
                rows = [dict(row) for row in cursor.fetchall()]
                conn.close()
                return rows
            
        return await asyncio.get_event_loop().run_in_executor(None, _get)
    
    async def get_time_series_messages(self, channel: str, domain: str, 
                                       start_time: str = None, end_time: str = None) -> List[Dict[str, Any]]:
        """Get message counts over time for charting.
        
        Returns hourly aggregated message counts.
        """
        def _get():
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # For now, use population snapshots as a proxy for activity
            # In the future, could track messages by timestamp
            query = """
                SELECT 
                    strftime('%Y-%m-%d %H:00:00', timestamp) as hour,
                    AVG(chat_count) as avg_active_users
                FROM population_snapshots
                WHERE channel = ? AND domain = ?
            """
            params = [channel, domain]
            
            if start_time:
                query += " AND datetime(timestamp) >= datetime(?)"
                params.append(start_time)
            if end_time:
                query += " AND datetime(timestamp) <= datetime(?)"
                params.append(end_time)
                
            query += " GROUP BY hour ORDER BY hour"
            
            cursor.execute(query, params)
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return rows
            
        return await asyncio.get_event_loop().run_in_executor(None, _get)
