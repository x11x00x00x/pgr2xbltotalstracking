const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const cors = require('cors');
const app = express();
const port = 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const db = new sqlite3.Database('xbltotal.db', (err) => {
    if (err) {
        console.error('Error connecting to database:', err.message);
    } else {
        console.log('Connected to the SQLite database.');
    }
});

// Helper function to execute queries
const runQuery = (query, params = []) => {
    return new Promise((resolve, reject) => {
        db.all(query, params, (err, rows) => {
            if (err) {
                reject(err);
            } else {
                resolve(rows);
            }
        });
    });
};

// Helper function to get the latest sync_id
const getLatestSyncId = async () => {
    const rows = await runQuery('SELECT sync_id FROM Sync ORDER BY sync_date DESC LIMIT 1');
    return rows.length > 0 ? rows[0].sync_id : null;
};

// Helper function to get the closest date for a given table and column
const getClosestDate = async (table, column, targetDate, extraConditions = '', params = []) => {
    // Find the row with the closest date to targetDate
    const query = `SELECT * FROM ${table} WHERE ${column} IS NOT NULL ${extraConditions} ORDER BY ABS(strftime('%s', ${column}) - strftime('%s', ?)) ASC LIMIT 1`;
    const rows = await runQuery(query, [...params, targetDate]);
    return rows.length > 0 ? rows[0][column] : null;
};

// Helper function to find the closest folder_date hour across all tables
const getClosestFolderDateHour = async (targetDate) => {
    // Decode URL-encoded characters (spaces, colons, etc.)
    let decodedDate = decodeURIComponent(targetDate);
    
    // Parse the target date - could be: "2024-11-26", "2024-11-26 17", or "2024-11-26 17:55:21"
    let targetDateTime = decodedDate;
    
    // If only date is provided, add default time
    if (decodedDate.match(/^\d{4}-\d{2}-\d{2}$/)) {
        targetDateTime = decodedDate + ' 12:00:00';
    }
    // If date and hour are provided, add default minutes/seconds
    else if (decodedDate.match(/^\d{4}-\d{2}-\d{2} \d{2}$/)) {
        targetDateTime = decodedDate + ':00:00';
    }
    // If date, hour, and minute are provided, add default seconds
    else if (decodedDate.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$/)) {
        targetDateTime = decodedDate + ':00';
    }
    
    // Extract the date and hour from target (format: "2024-11-26 17")
    const targetDateHour = targetDateTime.substring(0, 13); // "2024-11-26 17"
    
    // First, try to find exact hour match across all tables
    const hourPattern = targetDateHour + '%'; // "2024-11-26 17%"
    
    const exactHourQuery = `
        SELECT folder_date
        FROM (
            SELECT folder_date FROM XBLTotal WHERE folder_date LIKE ?
        )
        LIMIT 1
    `;
    
    let rows = await runQuery(exactHourQuery, [hourPattern]);
    
    // If no exact hour match, find the closest hour
    if (rows.length === 0) {
        const closestQuery = `
            SELECT folder_date, 
                   ABS(strftime('%s', folder_date) - strftime('%s', ?)) as time_diff
            FROM (
                SELECT folder_date FROM XBLTotal WHERE folder_date IS NOT NULL
            )
            ORDER BY time_diff ASC
            LIMIT 1
        `;
        
        rows = await runQuery(closestQuery, [targetDateTime]);
    }
    
    if (rows.length > 0) {
        // Get the hour from the found date (format: "2024-11-26 17")
        const foundDate = rows[0].folder_date;
        const foundHour = foundDate.substring(0, 13); // "2024-11-26 17"
        return foundHour;
    }
    
    return null;
};

// XBLTotal Endpoints
app.get('/api2/xbltotal', async (req, res) => {
    try {
        const { name, folder_date, data_date, sync_id, all } = req.query;
        let conditions = [];
        let params = [];

        if (name) {
            conditions.push('t.name LIKE ?');
            params.push(`%${name}%`);
        }
        
        // For date filtering, we'll filter after merging since dates might come from different tables
        // We'll handle this in a subquery or post-processing
        let useLatestSync = false;
        if (sync_id) {
            conditions.push('t.sync_id = ?');
            params.push(sync_id);
        } else if (!folder_date && !data_date && all !== 'true') {
            // Only use latest sync if 'all' parameter is not set
            useLatestSync = true;
        }

        // Build the base merged query
        // Start with just XBLTotal, then try to join other tables if they exist
        let query = `
            SELECT 
                t.id,
                t.leaderboard_id,
                t.rank,
                t.name,
                t.first_place_finishes,
                t.second_place_finishes,
                t.third_place_finishes,
                t.races_completed,
                t.kudos_rank,
                t.kudos,
                t.folder_date,
                t.data_date,
                t.sync_id
            FROM XBLTotal t
        `;

        // Apply latest sync_id filter if needed
        if (useLatestSync) {
            const latestSyncId = await getLatestSyncId();
            if (latestSyncId) {
                conditions.push('t.sync_id = ?');
                params.push(latestSyncId);
            }
        }

        if (conditions.length > 0) {
            query += ' WHERE ' + conditions.join(' AND ');
        }

        let rows = await runQuery(query, params);

        // Filter by dates if provided (post-query filtering for merged data)
        if (folder_date || data_date) {
            if (folder_date) {
                const closest = await getClosestDate('XBLTotal', 'folder_date', folder_date);
                if (closest) {
                    rows = rows.filter(row => row.folder_date === closest);
                }
            }
            if (data_date) {
                const closest = await getClosestDate('XBLTotal', 'data_date', data_date);
                if (closest) {
                    rows = rows.filter(row => row.data_date === closest);
                }
            }
        }

        res.json(rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Endpoint to get all unique dates
app.get('/api2/xbltotal/dates', async (req, res) => {
    try {
        const query = `
            SELECT DISTINCT DATE(folder_date) as date
            FROM XBLTotal
            WHERE folder_date IS NOT NULL
            ORDER BY date DESC
        `;
        const rows = await runQuery(query);
        const dates = rows.map(row => row.date);
        res.json(dates);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Helper function to check if a table exists
const tableExists = async (tableName) => {
    try {
        const query = `SELECT name FROM sqlite_master WHERE type='table' AND name=?`;
        const rows = await runQuery(query, [tableName]);
        return rows.length > 0;
    } catch (err) {
        return false;
    }
};

// Helper function to check if a column exists in a table
const columnExists = async (tableName, columnName) => {
    try {
        // PRAGMA doesn't support parameterized queries, so we need to sanitize the table name
        // Only allow alphanumeric and underscore characters
        if (!/^[a-zA-Z0-9_]+$/.test(tableName)) {
            return false;
        }
        const query = `PRAGMA table_info(${tableName})`;
        const rows = await runQuery(query, []);
        return rows.some(row => row.name === columnName);
    } catch (err) {
        return false;
    }
};

// Helper function to get the column name for a table (handles generic field names)
const getColumnName = (tableName, logicalColumnName) => {
    // XBLTotal1, XBLTotal2, XBLTotal3 use generic field names
    // Mapping: field1=id, field2=leaderboard_id, field3=rank, field4=name, 
    // field5=first_place_finishes, field6=second_place_finishes, field7=third_place_finishes,
    // field8=races_completed, field9=kudos_rank, field10=kudos, field11=folder_date,
    // field12=data_date, field13=sync_id
    const fieldMapping = {
        'id': 'field1',
        'leaderboard_id': 'field2',
        'rank': 'field3',
        'name': 'field4',
        'first_place_finishes': 'field5',
        'second_place_finishes': 'field6',
        'third_place_finishes': 'field7',
        'races_completed': 'field8',
        'kudos_rank': 'field9',
        'kudos': 'field10',
        'folder_date': 'field11',
        'data_date': 'field12',
        'sync_id': 'field13'
    };
    
    if (tableName === 'XBLTotal1' || tableName === 'XBLTotal2' || tableName === 'XBLTotal3') {
        return fieldMapping[logicalColumnName] || logicalColumnName;
    }
    
    // XBLTotal uses actual column names
    return logicalColumnName;
};

// Endpoint for chart data - returns all historical data with one data point per day per user (top 10 only)
app.get('/api2/xbltotal/chart', async (req, res) => {
    try {
        // Check which tables exist and have required columns
        const tableChecks = {
            XBLTotal: { exists: false, hasName: false, hasSyncId: false, hasFolderDate: false, hasKudos: false },
            XBLTotal1: { exists: false, hasName: false, hasSyncId: false, hasFolderDate: false, hasKudos: false },
            XBLTotal2: { exists: false, hasName: false, hasSyncId: false, hasFolderDate: false, hasKudos: false },
            XBLTotal3: { exists: false, hasName: false, hasSyncId: false, hasFolderDate: false, hasKudos: false }
        };
        
        for (const tableName of Object.keys(tableChecks)) {
            tableChecks[tableName].exists = await tableExists(tableName);
            if (tableChecks[tableName].exists) {
                // For XBLTotal1, XBLTotal2, XBLTotal3, check for field4 (name), field10 (kudos), field11 (folder_date), field13 (sync_id)
                if (tableName === 'XBLTotal1' || tableName === 'XBLTotal2' || tableName === 'XBLTotal3') {
                    tableChecks[tableName].hasName = await columnExists(tableName, 'field4');
                    tableChecks[tableName].hasSyncId = await columnExists(tableName, 'field13');
                    tableChecks[tableName].hasFolderDate = await columnExists(tableName, 'field11');
                    tableChecks[tableName].hasKudos = await columnExists(tableName, 'field10');
                } else {
                    // XBLTotal uses actual column names
                    tableChecks[tableName].hasName = await columnExists(tableName, 'name');
                    tableChecks[tableName].hasSyncId = await columnExists(tableName, 'sync_id');
                    tableChecks[tableName].hasFolderDate = await columnExists(tableName, 'folder_date');
                    tableChecks[tableName].hasKudos = await columnExists(tableName, 'kudos');
                }
            }
        }
        
        console.log('Table checks:', JSON.stringify(tableChecks, null, 2));
        
        // First, get the top 10 users by kudos from the latest sync
        // Only check tables that have sync_id, name, and kudos (XBLTotal, XBLTotal1, XBLTotal2)
        const latestSyncId = await getLatestSyncId();
        const top10Parts = [];
        const top10Params = [];
        
        for (const [tableName, checks] of Object.entries(tableChecks)) {
            if (checks.exists && checks.hasName && checks.hasSyncId && checks.hasKudos) {
                const nameCol = getColumnName(tableName, 'name');
                const kudosCol = getColumnName(tableName, 'kudos');
                const syncIdCol = getColumnName(tableName, 'sync_id');
                top10Parts.push(`SELECT ${nameCol} as name, ${kudosCol} as kudos FROM ${tableName} WHERE ${syncIdCol} = ?`);
                top10Params.push(latestSyncId || '');
                console.log(`Including ${tableName} in top 10 query`);
            }
        }
        
        if (top10Parts.length === 0) {
            console.log('No tables available for top 10 query');
            res.json({});
            return;
        }
        
        let top10Query = `
            SELECT name, MAX(kudos) as max_kudos
            FROM (
                ${top10Parts.join(' UNION ALL ')}
            )
            GROUP BY name
            ORDER BY max_kudos DESC
            LIMIT 10
        `;
        
        console.log('Top 10 query:', top10Query);
        const top10Rows = await runQuery(top10Query, top10Params);
        console.log(`Found ${top10Rows.length} top 10 users`);
        
        if (top10Rows.length === 0) {
            res.json({});
            return;
        }
        
        const top10Users = top10Rows.map(row => row.name);
        const placeholders = top10Users.map(() => '?').join(',');
        
        // Build query parts for historical data from all available tables
        // Include tables that have name, folder_date, and kudos (even if they don't have sync_id)
        const dataParts = [];
        const dataParams = [];
        
        for (const [tableName, checks] of Object.entries(tableChecks)) {
            if (checks.exists && checks.hasName && checks.hasFolderDate && checks.hasKudos) {
                const nameCol = getColumnName(tableName, 'name');
                const kudosCol = getColumnName(tableName, 'kudos');
                const folderDateCol = getColumnName(tableName, 'folder_date');
                // Ensure name is not null and not empty string
                dataParts.push(`SELECT ${folderDateCol} as folder_date, ${nameCol} as name, ${kudosCol} as kudos FROM ${tableName} WHERE ${folderDateCol} IS NOT NULL AND ${nameCol} IS NOT NULL AND ${nameCol} != '' AND ${nameCol} IN (${placeholders})`);
                dataParams.push(...top10Users);
                console.log(`Including ${tableName} in historical data query (using ${nameCol} for name)`);
            } else {
                console.log(`Skipping ${tableName}: exists=${checks.exists}, hasName=${checks.hasName}, hasFolderDate=${checks.hasFolderDate}, hasKudos=${checks.hasKudos}`);
            }
        }
        
        if (dataParts.length === 0) {
            console.log('No tables available for historical data query');
            res.json({});
            return;
        }
        
        console.log(`Building query with ${dataParts.length} table(s)`);
        
        // Query to get one data point per day per user (most recent for that day)
        // Make sure to select name explicitly and ensure it's not null, trim whitespace
        const query = `
            SELECT 
                DATE(t1.folder_date) as date,
                TRIM(COALESCE(t1.name, '')) as name,
                t1.kudos
            FROM (
                ${dataParts.join(' UNION ALL ')}
            ) t1
            INNER JOIN (
                SELECT 
                    TRIM(COALESCE(name, '')) as name,
                    DATE(folder_date) as date,
                    MAX(folder_date) as max_folder_date
                FROM (
                    ${dataParts.join(' UNION ALL ')}
                )
                WHERE name IS NOT NULL AND TRIM(COALESCE(name, '')) != ''
                GROUP BY TRIM(COALESCE(name, '')), DATE(folder_date)
            ) t2 ON TRIM(COALESCE(t1.name, '')) = t2.name 
                AND DATE(t1.folder_date) = t2.date 
                AND t1.folder_date = t2.max_folder_date
            WHERE t1.name IS NOT NULL AND TRIM(COALESCE(t1.name, '')) != ''
            ORDER BY date ASC, TRIM(COALESCE(t1.name, '')) ASC
        `;
        
        const rows = await runQuery(query, [...dataParams, ...dataParams]);
        console.log(`Retrieved ${rows.length} rows from historical data query`);
        
        // Debug: log first few rows to check structure
        if (rows.length > 0) {
            console.log('Sample row:', JSON.stringify(rows[0]));
        }
        
        // Organize data by user for easier chart consumption
        const chartData = {};
        
        rows.forEach(row => {
            // Ensure name is trimmed and not empty
            const userName = row.name ? String(row.name).trim() : null;
            if (!userName || userName === '') {
                console.warn('Row missing name:', JSON.stringify(row));
                return;
            }
            if (!chartData[userName]) {
                chartData[userName] = [];
            }
            
            chartData[userName].push({
                date: row.date,
                kudos: row.kudos || 0
            });
        });
        
        console.log(`Returning chart data for ${Object.keys(chartData).length} users`);
        res.json(chartData);
    } catch (err) {
        console.error('Error in chart endpoint:', err);
        res.status(500).json({ error: err.message });
    }
});

app.get('/api2/xbltotal/:date', async (req, res) => {
    try {
        const dateParam = req.params.date;
        
        // Find the closest folder_date hour across all tables
        const closestHour = await getClosestFolderDateHour(dateParam);
        
        if (!closestHour) {
            res.status(404).json({ error: 'No data found for the specified date' });
            return;
        }
        
        // Build query to merge data from all tables, filtering by the closest hour
        // Use LIKE to match the hour pattern (e.g., "2024-11-26 17%")
        const hourPattern = closestHour + '%';
        
        let query = `
            SELECT 
                t.id,
                t.leaderboard_id,
                t.rank,
                t.name,
                t.first_place_finishes,
                t.second_place_finishes,
                t.third_place_finishes,
                t.races_completed,
                t.kudos_rank,
                t.kudos,
                t.folder_date,
                t.data_date,
                t.sync_id
            FROM XBLTotal t
            WHERE t.folder_date LIKE ?
        `;
        
        const rows = await runQuery(query, [hourPattern]);
        
        // Deduplicate by name - if same name appears multiple times in the same hour, keep the most recent one
        const nameMap = new Map();
        
        rows.forEach(row => {
            const name = row.name;
            if (!name) return; // Skip rows without a name
            
            if (!nameMap.has(name)) {
                nameMap.set(name, row);
            } else {
                // If we already have this name, keep the one with the most recent folder_date
                const existing = nameMap.get(name);
                const currentDate = row.folder_date ? new Date(row.folder_date) : null;
                const existingDate = existing.folder_date ? new Date(existing.folder_date) : null;
                
                if (currentDate && (!existingDate || currentDate > existingDate)) {
                    nameMap.set(name, row);
                }
            }
        });
        
        const deduplicatedRows = Array.from(nameMap.values());
        
        if (deduplicatedRows.length === 0) {
            res.status(404).json({ error: 'No data found for the specified date' });
        } else {
            res.json(deduplicatedRows);
        }
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Start server
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});

