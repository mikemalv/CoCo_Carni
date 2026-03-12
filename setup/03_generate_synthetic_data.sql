-- =============================================================================
-- CARNIVAL CASINO SLOT ANALYTICS - SYNTHETIC DATA GENERATION
-- =============================================================================
-- This script generates realistic synthetic data for the casino analytics system
-- Run this AFTER 02_create_data_tables.sql
-- =============================================================================

USE SCHEMA CARNIVAL_CASINO.SLOT_ANALYTICS;
USE WAREHOUSE GEN2_SMALL;

-- =============================================================================
-- GENERATE MEMBER DEMOGRAPHICS (5,000 members)
-- =============================================================================
INSERT INTO MEMBER_DEMOGRAPHICS
WITH member_base AS (
    SELECT 
        SEQ4() + 1 AS MEMBER_ID,
        21 + ABS(MOD(RANDOM(), 60)) AS AGE,
        CASE MOD(ABS(RANDOM()), 2) WHEN 0 THEN 'M' ELSE 'F' END AS GENDER,
        CASE MOD(ABS(RANDOM()), 100)
            WHEN 0 THEN 'Platinum' WHEN 1 THEN 'Platinum' WHEN 2 THEN 'Platinum' WHEN 3 THEN 'Platinum' WHEN 4 THEN 'Platinum'
            WHEN 5 THEN 'Gold' WHEN 6 THEN 'Gold' WHEN 7 THEN 'Gold' WHEN 8 THEN 'Gold' WHEN 9 THEN 'Gold'
            WHEN 10 THEN 'Gold' WHEN 11 THEN 'Gold' WHEN 12 THEN 'Gold' WHEN 13 THEN 'Gold' WHEN 14 THEN 'Gold'
            WHEN 15 THEN 'Silver' WHEN 16 THEN 'Silver' WHEN 17 THEN 'Silver' WHEN 18 THEN 'Silver' WHEN 19 THEN 'Silver'
            WHEN 20 THEN 'Silver' WHEN 21 THEN 'Silver' WHEN 22 THEN 'Silver' WHEN 23 THEN 'Silver' WHEN 24 THEN 'Silver'
            WHEN 25 THEN 'Silver' WHEN 26 THEN 'Silver' WHEN 27 THEN 'Silver' WHEN 28 THEN 'Silver' WHEN 29 THEN 'Silver'
            WHEN 30 THEN 'Bronze' WHEN 31 THEN 'Bronze' WHEN 32 THEN 'Bronze' WHEN 33 THEN 'Bronze' WHEN 34 THEN 'Bronze'
            WHEN 35 THEN 'Bronze' WHEN 36 THEN 'Bronze' WHEN 37 THEN 'Bronze' WHEN 38 THEN 'Bronze' WHEN 39 THEN 'Bronze'
            WHEN 40 THEN 'Bronze' WHEN 41 THEN 'Bronze' WHEN 42 THEN 'Bronze' WHEN 43 THEN 'Bronze' WHEN 44 THEN 'Bronze'
            WHEN 45 THEN 'Bronze' WHEN 46 THEN 'Bronze' WHEN 47 THEN 'Bronze' WHEN 48 THEN 'Bronze' WHEN 49 THEN 'Bronze'
            ELSE 'Basic'
        END AS MEMBERSHIP_TIER,
        CASE MOD(ABS(RANDOM()), 10)
            WHEN 0 THEN 'FL' WHEN 1 THEN 'TX' WHEN 2 THEN 'CA' WHEN 3 THEN 'NY' WHEN 4 THEN 'GA'
            WHEN 5 THEN 'NC' WHEN 6 THEN 'PA' WHEN 7 THEN 'OH' WHEN 8 THEN 'AZ' ELSE 'NV'
        END AS HOME_STATE,
        DATEADD(DAY, -ABS(MOD(RANDOM(), 1825)), CURRENT_DATE()) AS ENROLLMENT_DATE,
        1 + ABS(MOD(RANDOM(), 20)) AS TOTAL_CRUISES,
        ROUND(1000 + ABS(MOD(RANDOM(), 50000))::FLOAT, 2) AS LIFETIME_SPEND,
        CASE MOD(ABS(RANDOM()), 4)
            WHEN 0 THEN 'Conservative' WHEN 1 THEN 'Moderate' WHEN 2 THEN 'Aggressive' ELSE 'High Roller'
        END AS RISK_APPETITE,
        CASE MOD(ABS(RANDOM()), 4)
            WHEN 0 THEN 'Single' WHEN 1 THEN 'Married' WHEN 2 THEN 'Divorced' ELSE 'Widowed'
        END AS MARITAL_STATUS,
        CASE MOD(ABS(RANDOM()), 5)
            WHEN 0 THEN 'Under $50K' WHEN 1 THEN '$50K-$100K' WHEN 2 THEN '$100K-$150K' WHEN 3 THEN '$150K-$250K' ELSE 'Over $250K'
        END AS INCOME_BRACKET
    FROM TABLE(GENERATOR(ROWCOUNT => 5000))
)
SELECT * FROM member_base;

-- =============================================================================
-- GENERATE SLOT PLAY HISTORY (100,000 sessions)
-- =============================================================================
INSERT INTO SLOT_PLAY_HISTORY
WITH play_base AS (
    SELECT 
        SEQ4() + 1 AS PLAY_ID,
        1 + ABS(MOD(RANDOM(), 5000)) AS MEMBER_ID,
        DATEADD(DAY, -ABS(MOD(RANDOM(), 730)), CURRENT_DATE()) AS PLAY_DATE,
        CASE MOD(ABS(RANDOM()), 8)
            WHEN 0 THEN 'Carnival Breeze' WHEN 1 THEN 'Carnival Magic' WHEN 2 THEN 'Carnival Vista'
            WHEN 3 THEN 'Carnival Horizon' WHEN 4 THEN 'Carnival Panorama' WHEN 5 THEN 'Carnival Celebration'
            WHEN 6 THEN 'Carnival Jubilee' ELSE 'Mardi Gras'
        END AS SHIP_NAME,
        CASE MOD(ABS(RANDOM()), 9)
            WHEN 0 THEN 0.01 WHEN 1 THEN 0.05 WHEN 2 THEN 0.25 WHEN 3 THEN 1.00
            WHEN 4 THEN 5.00 WHEN 5 THEN 10.00 WHEN 6 THEN 20.00 WHEN 7 THEN 50.00 ELSE 100.00
        END AS DENOMINATION,
        CASE MOD(ABS(RANDOM()), 15)
            WHEN 0 THEN 'Wheel of Fortune' WHEN 1 THEN 'Buffalo Gold' WHEN 2 THEN 'Lightning Link'
            WHEN 3 THEN 'Dragon Link' WHEN 4 THEN 'Cleopatra' WHEN 5 THEN 'Double Diamond'
            WHEN 6 THEN 'Quick Hit' WHEN 7 THEN 'Dancing Drums' WHEN 8 THEN 'Ocean Magic'
            WHEN 9 THEN 'Lock It Link' WHEN 10 THEN 'Wild Wild Nugget' WHEN 11 THEN 'Blazing 7s'
            WHEN 12 THEN 'Dolphin Treasure' WHEN 13 THEN 'More More Chilli' ELSE 'Ultimate Fire Link'
        END AS GAME_NAME,
        CASE MOD(ABS(RANDOM()), 4)
            WHEN 0 THEN 'Video Slots' WHEN 1 THEN 'Classic Slots' WHEN 2 THEN 'Progressive' ELSE 'Multi-Line'
        END AS GAME_TYPE,
        50 + ABS(MOD(RANDOM(), 450)) AS NUM_SPINS,
        CASE MOD(ABS(RANDOM()), 5)
            WHEN 0 THEN 1 WHEN 1 THEN 2 WHEN 2 THEN 3 WHEN 3 THEN 5 ELSE 10
        END AS BET_PER_SPIN,
        0 AS TOTAL_WAGERED,
        0 AS TOTAL_WON,
        0 AS NET_RESULT,
        10 + ABS(MOD(RANDOM(), 110)) AS SESSION_DURATION_MINS,
        CASE 
            WHEN MOD(ABS(RANDOM()), 100) < 10 THEN 'Morning'      -- 10%
            WHEN MOD(ABS(RANDOM()), 100) < 25 THEN 'Afternoon'    -- 15%
            WHEN MOD(ABS(RANDOM()), 100) < 55 THEN 'Evening'      -- 30%
            ELSE 'Night'                                           -- 45%
        END AS TIME_OF_DAY
    FROM TABLE(GENERATOR(ROWCOUNT => 100000))
)
SELECT 
    PLAY_ID, MEMBER_ID, PLAY_DATE, SHIP_NAME, DENOMINATION, GAME_NAME, GAME_TYPE, NUM_SPINS, BET_PER_SPIN,
    ROUND(NUM_SPINS * BET_PER_SPIN * DENOMINATION, 2) AS TOTAL_WAGERED,
    ROUND(NUM_SPINS * BET_PER_SPIN * DENOMINATION * (0.85 + (RANDOM() / 10000000000000000000::FLOAT) * 0.25), 2) AS TOTAL_WON,
    0 AS NET_RESULT,
    SESSION_DURATION_MINS, TIME_OF_DAY
FROM play_base;

-- Update NET_RESULT
UPDATE SLOT_PLAY_HISTORY SET NET_RESULT = TOTAL_WON - TOTAL_WAGERED;

-- =============================================================================
-- GENERATE CASINO POLICIES (15 policy documents)
-- =============================================================================
INSERT INTO CASINO_POLICIES VALUES
(1, 'Age Requirements', 'Minimum Gambling Age', 'All guests must be at least 21 years of age to participate in any casino gaming activities aboard Carnival cruise ships. Valid government-issued photo identification is required at all times while in the casino. Acceptable forms of ID include passport, driver''s license, or state ID card. Underage individuals are not permitted to loiter in the casino area during gaming hours.', '2024-01-15'),
(2, 'Age Requirements', 'Age Verification Process', 'Casino staff are trained to verify age through valid identification. Guests who appear to be under 30 years of age will be asked to present ID before being allowed to play. Players must keep their ID accessible while gaming. Failure to produce valid ID when requested will result in immediate removal from gaming activities.', '2024-01-15'),
(3, 'Responsible Gaming', 'Self-Exclusion Program', 'Carnival Cruise Line offers a voluntary self-exclusion program for guests who wish to limit their casino access. Guests may request self-exclusion for the duration of their cruise or for a specified period. Self-excluded individuals will be denied entry to the casino and removed if found gaming. To enroll, speak with Casino Management or Guest Services.', '2024-02-01'),
(4, 'Responsible Gaming', 'Problem Gambling Resources', 'Carnival is committed to responsible gaming. If you or someone you know has a gambling problem, please call the National Council on Problem Gambling helpline at 1-800-522-4700. Casino staff are trained to identify signs of problem gambling and can provide information about available resources. Guests may also set personal loss limits through Casino Management.', '2024-02-01'),
(5, 'Slot Machine Rules', 'General Slot Play Guidelines', 'All slot machines operate on a random number generator (RNG) system certified by independent testing laboratories. Minimum and maximum bets vary by machine and are clearly displayed. Players must insert their Ocean Players Club card to earn points and track play. Machine malfunctions void all pays and plays. Casino management decisions are final.', '2024-03-01'),
(6, 'Slot Machine Rules', 'Progressive Jackpot Terms', 'Progressive jackpots require maximum bet to be eligible for the top prize. Jackpot amounts displayed are approximate and subject to verification. Winners of jackpots over $1,200 will be required to provide tax documentation (W-2G). Jackpots may be paid by check or credited to onboard account based on amount and guest preference.', '2024-03-01'),
(7, 'Rewards Program', 'Ocean Players Club Overview', 'The Ocean Players Club is Carnival''s casino loyalty program. Members earn points based on slot play and table game wagers. Points can be redeemed for onboard credits, free play, and exclusive offers. Tier status (Basic, Bronze, Silver, Gold, Platinum) is based on annual play and provides escalating benefits including priority boarding, specialty dining discounts, and complimentary beverages.', '2024-01-01'),
(8, 'Rewards Program', 'Points Earning Structure', 'Slot players earn 1 point for every $5 wagered regardless of denomination. Table game players earn points based on average bet and time played as evaluated by pit supervisors. Points expire 24 months after last qualifying activity. Bonus point promotions are offered periodically and announced via Ocean Players Club communications.', '2024-01-01'),
(9, 'Casino Hours', 'Operating Schedule', 'The casino is open when the ship is in international waters, typically opening 1 hour after departure and closing 1 hour before arrival at US ports. In some international ports, the casino may remain open while docked. Specific hours are posted daily in the Carnival Hub app and at casino entrances. Tournament and special event schedules are posted in the casino.', '2024-01-01'),
(10, 'Payment Methods', 'Accepted Forms of Payment', 'The casino accepts cash (USD), Sail & Sign account charges, and casino markers for qualified players. Foreign currency is not accepted. ATM machines are available near the casino for cash advances. Credit card cash advances are subject to fees. Players may request chips or slot tickets at the casino cage.', '2024-02-15'),
(11, 'Payment Methods', 'Jackpot Payments', 'Jackpots under $1,200 are paid immediately in cash or slot ticket. Jackpots of $1,200 or more require tax documentation and may be paid by check. Non-US citizens may be subject to 30% withholding unless a tax treaty applies. All jackpots are subject to verification and may require hand pay by casino personnel.', '2024-02-15'),
(12, 'Conduct & Etiquette', 'Casino Code of Conduct', 'Guests are expected to conduct themselves in a respectful manner at all times. Harassment of staff or other guests will not be tolerated. Excessive intoxication may result in removal from the casino. Dress code is resort casual; swimwear and bare feet are not permitted. Cell phone use is allowed but photography/video of gaming areas is prohibited.', '2024-01-01'),
(13, 'Conduct & Etiquette', 'Machine Reservation Policy', 'Players may reserve slot machines for brief breaks (restroom, drinks) by leaving their players club card in the machine and notifying an attendant. Reservations are limited to 15 minutes. Unattended machines with credits may be cashed out by casino staff after 30 minutes. Players should not leave personal belongings unattended.', '2024-01-01'),
(14, 'Tournaments', 'Slot Tournament Rules', 'Slot tournaments are offered throughout each cruise with varying buy-ins and prize pools. Tournament schedules and registration are available at the casino podium. Players compete for the highest score within a set time limit using tournament credits (not real money). Prizes are typically awarded as onboard credit or free play.', '2024-03-15'),
(15, 'Tournaments', 'Table Game Tournament Rules', 'Blackjack and poker tournaments feature elimination rounds with advancing players competing for prize pools. Tournament chips have no cash value. Specific rules for each tournament type are provided at registration. Seat assignments are random. Tournament supervisors'' decisions are final regarding rule interpretations and disputes.', '2024-03-15');

-- =============================================================================
-- SHIP-SPECIFIC DENOMINATION REDISTRIBUTION
-- =============================================================================
-- Each ship has a distinct passenger demographic that drives denomination demand.
-- We use a CDF-based sampling approach to reassign denominations per ship,
-- replacing the original uniform distribution with realistic ship profiles.
--
-- Ship profiles:
--   Carnival Breeze: Budget/family ship → heavy penny/nickel/quarter
--   Mardi Gras: Flagship/premium → heavy $50/$20/$100
--   Carnival Jubilee: High rollers → $20/$10/$50 focused
--   Carnival Horizon: Balanced mid-range → $1/$5/quarter
--   Others: Mix of low-to-mid denominations

CREATE TABLE IF NOT EXISTS SLOT_PLAY_HISTORY_OLD AS SELECT * FROM SLOT_PLAY_HISTORY;

CREATE OR REPLACE TABLE SLOT_PLAY_HISTORY AS
WITH ship_weights AS (
    SELECT *
    FROM VALUES
        ('Carnival Breeze',   0.01,  25.0),
        ('Carnival Breeze',   0.05,  20.0),
        ('Carnival Breeze',   0.25,  20.0),
        ('Carnival Breeze',   1.00,  15.0),
        ('Carnival Breeze',   5.00,   8.0),
        ('Carnival Breeze',  10.00,   5.0),
        ('Carnival Breeze',  20.00,   3.5),
        ('Carnival Breeze',  50.00,   2.0),
        ('Carnival Breeze', 100.00,   1.5),
        ('Mardi Gras',       0.01,   2.0),
        ('Mardi Gras',       0.05,   3.0),
        ('Mardi Gras',       0.25,   5.0),
        ('Mardi Gras',       1.00,   8.0),
        ('Mardi Gras',       5.00,  12.0),
        ('Mardi Gras',      10.00,  15.0),
        ('Mardi Gras',      20.00,  18.0),
        ('Mardi Gras',      50.00,  22.0),
        ('Mardi Gras',     100.00,  15.0),
        ('Carnival Jubilee',  0.01,   3.0),
        ('Carnival Jubilee',  0.05,   4.0),
        ('Carnival Jubilee',  0.25,   6.0),
        ('Carnival Jubilee',  1.00,  10.0),
        ('Carnival Jubilee',  5.00,  12.0),
        ('Carnival Jubilee', 10.00,  17.0),
        ('Carnival Jubilee', 20.00,  18.0),
        ('Carnival Jubilee', 50.00,  16.0),
        ('Carnival Jubilee',100.00,  14.0),
        ('Carnival Horizon',  0.01,   8.0),
        ('Carnival Horizon',  0.05,  10.0),
        ('Carnival Horizon',  0.25,  14.0),
        ('Carnival Horizon',  1.00,  18.0),
        ('Carnival Horizon',  5.00,  16.0),
        ('Carnival Horizon', 10.00,  13.0),
        ('Carnival Horizon', 20.00,   9.0),
        ('Carnival Horizon', 50.00,   7.0),
        ('Carnival Horizon',100.00,   5.0),
        ('Carnival Magic',    0.01,  20.0),
        ('Carnival Magic',    0.05,  17.0),
        ('Carnival Magic',    0.25,  15.0),
        ('Carnival Magic',    1.00,  14.0),
        ('Carnival Magic',    5.00,  12.0),
        ('Carnival Magic',   10.00,   8.0),
        ('Carnival Magic',   20.00,   6.0),
        ('Carnival Magic',   50.00,   4.0),
        ('Carnival Magic',  100.00,   4.0),
        ('Carnival Vista',    0.01,  15.0),
        ('Carnival Vista',    0.05,  14.0),
        ('Carnival Vista',    0.25,  13.0),
        ('Carnival Vista',    1.00,  14.0),
        ('Carnival Vista',    5.00,  13.0),
        ('Carnival Vista',   10.00,  11.0),
        ('Carnival Vista',   20.00,   8.0),
        ('Carnival Vista',   50.00,   7.0),
        ('Carnival Vista',  100.00,   5.0),
        ('Carnival Panorama', 0.01,   8.0),
        ('Carnival Panorama', 0.05,  10.0),
        ('Carnival Panorama', 0.25,  12.0),
        ('Carnival Panorama', 1.00,  15.0),
        ('Carnival Panorama', 5.00,  15.0),
        ('Carnival Panorama',10.00,  14.0),
        ('Carnival Panorama',20.00,  10.0),
        ('Carnival Panorama',50.00,   9.0),
        ('Carnival Panorama',100.00,  7.0),
        ('Carnival Celebration', 0.01,  10.0),
        ('Carnival Celebration', 0.05,  12.0),
        ('Carnival Celebration', 0.25,  13.0),
        ('Carnival Celebration', 1.00,  15.0),
        ('Carnival Celebration', 5.00,  14.0),
        ('Carnival Celebration',10.00,  12.0),
        ('Carnival Celebration',20.00,   9.0),
        ('Carnival Celebration',50.00,   8.0),
        ('Carnival Celebration',100.00,  7.0)
    AS t(ship_name, denom, weight)
),
ship_cdf AS (
    SELECT
        ship_name, denom, weight,
        SUM(weight) OVER (PARTITION BY ship_name ORDER BY denom ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            / SUM(weight) OVER (PARTITION BY ship_name) AS cdf_upper
    FROM ship_weights
),
plays_with_rand AS (
    SELECT *, ABS(MOD(RANDOM(), 10000)) / 10000.0 AS rand_val
    FROM SLOT_PLAY_HISTORY_OLD
)
SELECT
    p.PLAY_ID, p.MEMBER_ID, p.PLAY_DATE, p.SHIP_NAME,
    c.denom AS DENOMINATION,
    p.GAME_NAME, p.GAME_TYPE, p.NUM_SPINS, p.BET_PER_SPIN,
    ROUND(p.NUM_SPINS * p.BET_PER_SPIN * c.denom, 2) AS TOTAL_WAGERED,
    ROUND(p.NUM_SPINS * p.BET_PER_SPIN * c.denom * (0.85 + ABS(MOD(RANDOM(), 2500)) / 10000.0), 2) AS TOTAL_WON,
    0 AS NET_RESULT,
    p.SESSION_DURATION_MINS, p.TIME_OF_DAY
FROM plays_with_rand p
JOIN ship_cdf c
    ON p.SHIP_NAME = c.ship_name
    AND c.cdf_upper >= p.rand_val
    AND c.denom = (
        SELECT MIN(c2.denom) FROM ship_cdf c2
        WHERE c2.ship_name = p.SHIP_NAME AND c2.cdf_upper >= p.rand_val
    );

UPDATE SLOT_PLAY_HISTORY SET NET_RESULT = TOTAL_WON - TOTAL_WAGERED;

-- =============================================================================
-- CREATE SHIP_BANK_DEMAND TABLE
-- =============================================================================
-- Aggregated denomination demand per ship with passenger demographic features.
-- Used by the Bank Denomination Allocator in the Streamlit app and
-- as training data for the BANK_DENOMINATION_MODEL.

CREATE OR REPLACE TABLE SHIP_BANK_DEMAND AS
WITH ship_denom_stats AS (
    SELECT
        SHIP_NAME,
        DENOMINATION,
        COUNT(*) AS SESSION_COUNT,
        ROUND(SUM(TOTAL_WAGERED), 2) AS TOTAL_WAGERED,
        ROUND(AVG(NUM_SPINS), 0) AS AVG_SPINS,
        ROUND(AVG(BET_PER_SPIN), 2) AS AVG_BET,
        COUNT(DISTINCT MEMBER_ID) AS UNIQUE_PLAYERS
    FROM SLOT_PLAY_HISTORY
    GROUP BY SHIP_NAME, DENOMINATION
),
ship_totals AS (
    SELECT SHIP_NAME, SUM(SESSION_COUNT) AS TOTAL_SESSIONS, SUM(TOTAL_WAGERED) AS SHIP_WAGERED
    FROM ship_denom_stats
    GROUP BY SHIP_NAME
),
ship_demographics AS (
    SELECT
        p.SHIP_NAME,
        ROUND(AVG(m.AGE), 1) AS AVG_PASSENGER_AGE,
        ROUND(SUM(CASE WHEN m.GENDER = 'M' THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS PCT_MALE,
        ROUND(SUM(CASE WHEN m.MEMBERSHIP_TIER IN ('Gold','Platinum') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS PCT_HIGH_TIER,
        ROUND(SUM(CASE WHEN m.INCOME_BRACKET IN ('$150K-$250K','Over $250K') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS PCT_HIGH_INCOME,
        ROUND(SUM(CASE WHEN m.RISK_APPETITE IN ('Aggressive','High Roller') THEN 1 ELSE 0 END)::FLOAT / COUNT(*), 3) AS PCT_HIGH_RISK,
        ROUND(AVG(m.TOTAL_CRUISES), 1) AS AVG_CRUISES,
        ROUND(AVG(m.LIFETIME_SPEND), 0) AS AVG_LIFETIME_SPEND
    FROM SLOT_PLAY_HISTORY p
    JOIN MEMBER_DEMOGRAPHICS m ON p.MEMBER_ID = m.MEMBER_ID
    GROUP BY p.SHIP_NAME
)
SELECT
    s.SHIP_NAME,
    s.DENOMINATION,
    s.SESSION_COUNT,
    ROUND(s.SESSION_COUNT * 100.0 / t.TOTAL_SESSIONS, 2) AS DEMAND_PCT,
    s.TOTAL_WAGERED,
    ROUND(s.TOTAL_WAGERED * 100.0 / t.SHIP_WAGERED, 2) AS WAGERED_PCT,
    s.AVG_SPINS,
    s.AVG_BET,
    d.UNIQUE_PLAYERS,
    d.AVG_PASSENGER_AGE,
    d.PCT_MALE,
    d.PCT_HIGH_TIER,
    d.PCT_HIGH_INCOME,
    d.PCT_HIGH_RISK,
    d.AVG_CRUISES,
    d.AVG_LIFETIME_SPEND
FROM ship_denom_stats s
JOIN ship_totals t ON s.SHIP_NAME = t.SHIP_NAME
JOIN ship_demographics d ON s.SHIP_NAME = d.SHIP_NAME
ORDER BY s.SHIP_NAME, s.DENOMINATION;

-- =============================================================================
-- GENERATE CASINO POLICIES (15 policy documents)
-- =============================================================================
INSERT INTO CASINO_POLICIES VALUES
(1, 'Age Requirements', 'Minimum Gambling Age', 'All guests must be at least 21 years of age to participate in any casino gaming activities aboard Carnival cruise ships. Valid government-issued photo identification is required at all times while in the casino. Acceptable forms of ID include passport, driver''s license, or state ID card. Underage individuals are not permitted to loiter in the casino area during gaming hours.', '2024-01-15'),
(2, 'Age Requirements', 'Age Verification Process', 'Casino staff are trained to verify age through valid identification. Guests who appear to be under 30 years of age will be asked to present ID before being allowed to play. Players must keep their ID accessible while gaming. Failure to produce valid ID when requested will result in immediate removal from gaming activities.', '2024-01-15'),
(3, 'Responsible Gaming', 'Self-Exclusion Program', 'Carnival Cruise Line offers a voluntary self-exclusion program for guests who wish to limit their casino access. Guests may request self-exclusion for the duration of their cruise or for a specified period. Self-excluded individuals will be denied entry to the casino and removed if found gaming. To enroll, speak with Casino Management or Guest Services.', '2024-02-01'),
(4, 'Responsible Gaming', 'Problem Gambling Resources', 'Carnival is committed to responsible gaming. If you or someone you know has a gambling problem, please call the National Council on Problem Gambling helpline at 1-800-522-4700. Casino staff are trained to identify signs of problem gambling and can provide information about available resources. Guests may also set personal loss limits through Casino Management.', '2024-02-01'),
(5, 'Slot Machine Rules', 'General Slot Play Guidelines', 'All slot machines operate on a random number generator (RNG) system certified by independent testing laboratories. Minimum and maximum bets vary by machine and are clearly displayed. Players must insert their Ocean Players Club card to earn points and track play. Machine malfunctions void all pays and plays. Casino management decisions are final.', '2024-03-01'),
(6, 'Slot Machine Rules', 'Progressive Jackpot Terms', 'Progressive jackpots require maximum bet to be eligible for the top prize. Jackpot amounts displayed are approximate and subject to verification. Winners of jackpots over $1,200 will be required to provide tax documentation (W-2G). Jackpots may be paid by check or credited to onboard account based on amount and guest preference.', '2024-03-01'),
(7, 'Rewards Program', 'Ocean Players Club Overview', 'The Ocean Players Club is Carnival''s casino loyalty program. Members earn points based on slot play and table game wagers. Points can be redeemed for onboard credits, free play, and exclusive offers. Tier status (Basic, Bronze, Silver, Gold, Platinum) is based on annual play and provides escalating benefits including priority boarding, specialty dining discounts, and complimentary beverages.', '2024-01-01'),
(8, 'Rewards Program', 'Points Earning Structure', 'Slot players earn 1 point for every $5 wagered regardless of denomination. Table game players earn points based on average bet and time played as evaluated by pit supervisors. Points expire 24 months after last qualifying activity. Bonus point promotions are offered periodically and announced via Ocean Players Club communications.', '2024-01-01'),
(9, 'Casino Hours', 'Operating Schedule', 'The casino is open when the ship is in international waters, typically opening 1 hour after departure and closing 1 hour before arrival at US ports. In some international ports, the casino may remain open while docked. Specific hours are posted daily in the Carnival Hub app and at casino entrances. Tournament and special event schedules are posted in the casino.', '2024-01-01'),
(10, 'Payment Methods', 'Accepted Forms of Payment', 'The casino accepts cash (USD), Sail & Sign account charges, and casino markers for qualified players. Foreign currency is not accepted. ATM machines are available near the casino for cash advances. Credit card cash advances are subject to fees. Players may request chips or slot tickets at the casino cage.', '2024-02-15'),
(11, 'Payment Methods', 'Jackpot Payments', 'Jackpots under $1,200 are paid immediately in cash or slot ticket. Jackpots of $1,200 or more require tax documentation and may be paid by check. Non-US citizens may be subject to 30% withholding unless a tax treaty applies. All jackpots are subject to verification and may require hand pay by casino personnel.', '2024-02-15'),
(12, 'Conduct & Etiquette', 'Casino Code of Conduct', 'Guests are expected to conduct themselves in a respectful manner at all times. Harassment of staff or other guests will not be tolerated. Excessive intoxication may result in removal from the casino. Dress code is resort casual; swimwear and bare feet are not permitted. Cell phone use is allowed but photography/video of gaming areas is prohibited.', '2024-01-01'),
(13, 'Conduct & Etiquette', 'Machine Reservation Policy', 'Players may reserve slot machines for brief breaks (restroom, drinks) by leaving their players club card in the machine and notifying an attendant. Reservations are limited to 15 minutes. Unattended machines with credits may be cashed out by casino staff after 30 minutes. Players should not leave personal belongings unattended.', '2024-01-01'),
(14, 'Tournaments', 'Slot Tournament Rules', 'Slot tournaments are offered throughout each cruise with varying buy-ins and prize pools. Tournament schedules and registration are available at the casino podium. Players compete for the highest score within a set time limit using tournament credits (not real money). Prizes are typically awarded as onboard credit or free play.', '2024-03-15'),
(15, 'Tournaments', 'Table Game Tournament Rules', 'Blackjack and poker tournaments feature elimination rounds with advancing players competing for prize pools. Tournament chips have no cash value. Specific rules for each tournament type are provided at registration. Seat assignments are random. Tournament supervisors'' decisions are final regarding rule interpretations and disputes.', '2024-03-15');

-- =============================================================================
-- VERIFY DATA GENERATION
-- =============================================================================
SELECT 'MEMBER_DEMOGRAPHICS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM MEMBER_DEMOGRAPHICS
UNION ALL
SELECT 'SLOT_PLAY_HISTORY', COUNT(*) FROM SLOT_PLAY_HISTORY
UNION ALL
SELECT 'CASINO_POLICIES', COUNT(*) FROM CASINO_POLICIES
UNION ALL
SELECT 'SHIP_BANK_DEMAND', COUNT(*) FROM SHIP_BANK_DEMAND
UNION ALL
SELECT 'VOYAGE_PROFIT_TRAINING', COUNT(*) FROM VOYAGE_PROFIT_TRAINING;

-- =============================================================================
-- GENERATE VOYAGE PROFIT TRAINING DATA
-- Rolling voyage windows (3-7 days) with passenger demographics for profit prediction
-- =============================================================================
CREATE OR REPLACE TABLE VOYAGE_PROFIT_TRAINING AS
WITH voyage_windows AS (
    SELECT
        sdm.SHIP_NAME,
        sdm.PLAY_DATE AS DEPARTURE_DATE,
        dur.DURATION AS VOYAGE_DURATION,
        MONTH(sdm.PLAY_DATE) AS DEPARTURE_MONTH,
        DAYOFWEEK(sdm.PLAY_DATE) AS DEPARTURE_DOW
    FROM SHIP_DAILY_METRICS sdm
    CROSS JOIN (SELECT 3 AS DURATION UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7) dur
    WHERE sdm.PLAY_DATE <= DATEADD('day', -7, (SELECT MAX(PLAY_DATE) FROM SHIP_DAILY_METRICS))
),
voyage_metrics AS (
    SELECT
        vw.SHIP_NAME, vw.DEPARTURE_DATE, vw.VOYAGE_DURATION,
        vw.DEPARTURE_MONTH, vw.DEPARTURE_DOW,
        SUM(sdm.DAILY_WAGERED) AS TOTAL_WAGERED,
        SUM(sdm.DAILY_WON) AS TOTAL_WON,
        SUM(sdm.DAILY_WAGERED - sdm.DAILY_WON) AS TOTAL_PROFIT,
        AVG(sdm.TOTAL_SESSIONS) AS AVG_DAILY_SESSIONS,
        AVG(sdm.UNIQUE_PLAYERS) AS AVG_DAILY_PLAYERS
    FROM voyage_windows vw
    JOIN SHIP_DAILY_METRICS sdm
      ON sdm.SHIP_NAME = vw.SHIP_NAME
     AND sdm.PLAY_DATE BETWEEN vw.DEPARTURE_DATE
         AND DATEADD('day', vw.VOYAGE_DURATION - 1, vw.DEPARTURE_DATE)
    GROUP BY 1, 2, 3, 4, 5
),
voyage_demographics AS (
    SELECT
        vw.SHIP_NAME, vw.DEPARTURE_DATE, vw.VOYAGE_DURATION,
        AVG(m.AGE) AS AVG_PASSENGER_AGE,
        SUM(CASE WHEN m.GENDER = 'M' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(DISTINCT sph.MEMBER_ID), 0) AS PCT_MALE,
        SUM(CASE WHEN m.MEMBERSHIP_TIER IN ('Platinum', 'Diamond') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(DISTINCT sph.MEMBER_ID), 0) AS PCT_HIGH_TIER,
        SUM(CASE WHEN m.INCOME_BRACKET IN ('$100K-$150K', 'Over $150K') THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(DISTINCT sph.MEMBER_ID), 0) AS PCT_HIGH_INCOME,
        SUM(CASE WHEN m.RISK_APPETITE = 'High' THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(DISTINCT sph.MEMBER_ID), 0) AS PCT_HIGH_RISK,
        AVG(m.TOTAL_CRUISES) AS AVG_CRUISES,
        AVG(m.LIFETIME_SPEND) AS AVG_LIFETIME_SPEND
    FROM voyage_windows vw
    JOIN SLOT_PLAY_HISTORY sph
      ON sph.SHIP_NAME = vw.SHIP_NAME
     AND sph.PLAY_DATE BETWEEN vw.DEPARTURE_DATE
         AND DATEADD('day', vw.VOYAGE_DURATION - 1, vw.DEPARTURE_DATE)
    JOIN MEMBER_DEMOGRAPHICS m ON sph.MEMBER_ID = m.MEMBER_ID
    GROUP BY 1, 2, 3
)
SELECT
    vm.SHIP_NAME, vm.DEPARTURE_DATE, vm.VOYAGE_DURATION,
    vm.DEPARTURE_MONTH, vm.DEPARTURE_DOW,
    vm.TOTAL_WAGERED, vm.TOTAL_WON, vm.TOTAL_PROFIT,
    vm.AVG_DAILY_SESSIONS, vm.AVG_DAILY_PLAYERS,
    COALESCE(vd.AVG_PASSENGER_AGE, 50) AS AVG_PASSENGER_AGE,
    COALESCE(vd.PCT_MALE, 0.5) AS PCT_MALE,
    COALESCE(vd.PCT_HIGH_TIER, 0.2) AS PCT_HIGH_TIER,
    COALESCE(vd.PCT_HIGH_INCOME, 0.2) AS PCT_HIGH_INCOME,
    COALESCE(vd.PCT_HIGH_RISK, 0.3) AS PCT_HIGH_RISK,
    COALESCE(vd.AVG_CRUISES, 10) AS AVG_CRUISES,
    COALESCE(vd.AVG_LIFETIME_SPEND, 20000) AS AVG_LIFETIME_SPEND
FROM voyage_metrics vm
LEFT JOIN voyage_demographics vd
  ON vm.SHIP_NAME = vd.SHIP_NAME
 AND vm.DEPARTURE_DATE = vd.DEPARTURE_DATE
 AND vm.VOYAGE_DURATION = vd.VOYAGE_DURATION
ORDER BY vm.SHIP_NAME, vm.DEPARTURE_DATE, vm.VOYAGE_DURATION;
