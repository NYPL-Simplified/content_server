-- Update any 'Short Stories' subjects used ONLY by
-- FeedBooks to connect with the 'Short Stories' genre.
-- This will allow Works of short stories to be organized
-- as such in the lanes.

UPDATE subjects
SET genre_id = (SELECT genres.id FROM genres WHERE genres.name = 'Short Stories')
WHERE id in (
    SELECT s.id FROM (
        -- All 'short stories' subjects used by FeedBooks.
        SELECT s.id FROM subjects s
        JOIN classifications c ON s.id = c.subject_id
        JOIN datasources ds ON c.data_source_id = ds.id
        WHERE s.name = 'Short Stories' and ds.name = 'FeedBooks'
        GROUP BY s.id)
    AS feedbooks_short_stories_subjects
    JOIN subjects s on feedbooks_short_stories_subjects.id = s.id
    JOIN classifications c ON s.id = c.subject_id
    JOIN datasources ds ON c.data_source_id = ds.id
    GROUP BY s.id
    -- And only by FeedBooks.
    HAVING count(distinct ds.id) = 1);
