const puppeteer = require('puppeteer');
const cheerio = require('cheerio');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { v4: uuidv4 } = require('uuid');

// Constants
const DB_PATH = path.join(__dirname, 'xbltotal.db');
const url = "https://insignia.live/games/4d53004b";

// Helper function to wait for table to reload
async function waitForTableToReload(page) {
    await page.waitForFunction(() => {
        const rows = document.querySelectorAll('table.table-striped tbody tr');
        return rows.length > 0;
    }, { timeout: 5000 });
}

// Helper function to sleep
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Initialize database connection
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('Error opening database:', err);
    } else {
        console.log('Connected to the SQLite database.');
    }
});

async function fetchData() {
    // Generate a unique sync_id for this run
    const sync_id = uuidv4();
    const sync_date = new Date().toISOString();

    // Insert sync record
    const syncStmt = db.prepare('INSERT INTO Sync (sync_id, sync_date) VALUES (?, ?)');
    syncStmt.run(sync_id, sync_date);
    syncStmt.finalize();

    const browser = await puppeteer.launch({
        headless: true,
        defaultViewport: null,
        args: ['--no-sandbox']
    });
    const page = await browser.newPage();

    try {
        // Navigate to the URL
        await page.goto(url, { waitUntil: 'networkidle2' });
        console.log('Navigated to URL successfully');

        // Get all options from the select box
        const options = await page.evaluate(() => {
            const selectElement = document.getElementById('leaderboard-select');
            return Array.from(selectElement.options).map(option => ({
                value: option.value,
                text: option.text
            }));
        });
        console.log('Found options:', options);

        // Find and process only leaderboard ID 1
        const leaderboard1Option = options.find(opt => parseInt(opt.value) === 1);
        
        if (!leaderboard1Option) {
            console.error('Leaderboard ID 1 not found in options');
            await browser.close();
            return;
        }

        console.log(`Processing leaderboard ${leaderboard1Option.text} (ID: 1)`);
        
        await page.select('#leaderboard-select', leaderboard1Option.value);
        await sleep(1000);
        await waitForTableToReload(page);

        // Handle XBLTotal table (leaderboard ID 1)
        const leaderboardData = await page.evaluate(() => {
            const rows = Array.from(document.querySelectorAll('table.table-striped tbody tr'));
            if (!rows || rows.length === 0) return [];
            
            return rows.map(row => {
                const cells = row.querySelectorAll('td');
                if (!cells || cells.length < 8) return null;
                
                try {
                    return {
                        rank: parseInt(cells[0].textContent.trim()) || 0,
                        name: cells[1].textContent.trim() || '',
                        first_place_finishes: parseInt(cells[2].textContent.trim()) || 0,
                        second_place_finishes: parseInt(cells[3].textContent.trim()) || 0,
                        third_place_finishes: parseInt(cells[4].textContent.trim()) || 0,
                        races_completed: parseInt(cells[5].textContent.trim()) || 0,
                        kudos_rank: parseInt(cells[6].textContent.trim()) || 0,
                        kudos: parseInt(cells[7].textContent.trim()) || 0
                    };
                } catch (error) {
                    console.error('Error parsing row:', error);
                    return null;
                }
            }).filter(item => item !== null);
        });

        if (leaderboardData && leaderboardData.length > 0) {
            const stmt = db.prepare(`
                INSERT INTO XBLTotal (
                    leaderboard_id, rank, name, first_place_finishes, 
                    second_place_finishes, third_place_finishes, races_completed,
                    kudos_rank, kudos, folder_date, data_date, sync_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'), ?)
            `);

            leaderboardData.forEach(entry => {
                stmt.run(
                    1,
                    entry.rank,
                    entry.name,
                    entry.first_place_finishes,
                    entry.second_place_finishes,
                    entry.third_place_finishes,
                    entry.races_completed,
                    entry.kudos_rank,
                    entry.kudos,
                    sync_id
                );
            });
            
            stmt.finalize();
            console.log(`Inserted ${leaderboardData.length} XBLTotal records`);
        }

        await browser.close();
    } catch (error) {
        console.error('Error:', error);
        await browser.close();
    }
}

// Main function to run everything
async function runAll() {
    try {
        await fetchData();
        console.log('Data collection completed successfully');
    } catch (err) {
        console.error('Error in main process:', err);
    } finally {
        db.close((err) => {
            if (err) {
                console.error('Error closing database:', err);
            } else {
                console.log('Database connection closed');
            }
        });
    }
}

// Run the script
runAll();

