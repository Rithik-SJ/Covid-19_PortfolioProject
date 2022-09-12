
--Using SELECT to view the tables created

SELECT * FROM Portfolioproject_01..covid_deaths$
ORDER BY 3,4;

SELECT * FROM Portfolioproject_01..Covid_vaccinations$
ORDER BY 3,4;


SELECT Location, date, new_cases, total_cases,total_deaths, population
FROM Portfolioproject_01..covid_deaths$
WHERE continent is Not NULL
ORDER BY 1,2;

-- TotalDeaths vs Totalcases

SELECT Location, date, population, new_cases, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
WHERE continent is Not NULL
ORDER BY 1,2;

-- Location specific :: India

SELECT Location, date, population, new_cases, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
WHERE location = 'India'
AND continent is Not NULL
ORDER BY 1,2;

--Death_rate (WHEN IT REACHES MAXIMUM ALONG WITH THE DATE)

SELECT Location, date, population, new_cases, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
WHERE location = 'India'
AND continent is Not NULL
ORDER BY 7 DESC;

--INFECTED PERCENTAGE ON HOW IT INCREASES AND DROPS

SELECT Location, date, population, total_cases, (total_cases/population)*100 AS Percent_Infected
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
ORDER BY 1,2;


--Looking at Countries highest Infection percentage compared to population

SELECT Location, population, MAX(total_cases) AS total_cases, MAX((total_cases/population))*100 AS Percent_Infected
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
GROUP BY Location, population
ORDER BY Percent_Infected DESC;


--Looking at Percent of population death due to covid
--(total_deaths/population)*100 ;Death_rate

SELECT Location, population, MAX(CAST(total_deaths AS int)) AS total_deaths, MAX((total_deaths/population))*100 AS Death_rate
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is Not NULL
GROUP BY Location, population
ORDER BY Death_rate DESC;

-- Just the deathcount w.r.t location 

SELECT Location, MAX(CAST(total_deaths AS int)) AS total_deaths_count
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is Not NULL
GROUP BY Location
ORDER BY total_deaths_count DESC;


--BREAKING IT DOWN INTO CONTINENTS

SELECT continent, MAX(CAST(total_deaths AS int)) AS total_deaths_count
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is Not NULL
GROUP BY continent
ORDER BY total_deaths_count DESC;

--ON THE CONDITION OF CONTINENTS COLUMN VALUE NULL 

SELECT location, MAX(CAST(total_deaths AS int)) AS total_deaths_count
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is NULL
GROUP BY location
ORDER BY total_deaths_count DESC;


--PERCENTAGE OF DEATHS ACROSS THE WORLD--

SELECT date,SUM(new_cases) as total_cases, SUM(CAST(new_deaths as int)) AS total_deaths, 
(SUM(CAST(new_deaths as int))/SUM(new_cases))*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is NOT NULL
GROUP BY date HAVING SUM(new_cases) IS NOT NULL
ORDER BY 1,2;

-- ON TOTAL ACROSS THE WORLD

SELECT SUM(new_cases) as total_cases, SUM(CAST(new_deaths as int)) AS total_deaths, 
(SUM(CAST(new_deaths as int))/SUM(new_cases))*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
--WHERE continent is NOT NULL
--GROUP BY date HAVING SUM(new_cases) IS NOT NULL
ORDER BY 1,2;

--Looking at vaccination column

SELECT Location, date, population, people_vaccinated, total_vaccinations, new_vaccinations
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is Not NULL
AND total_vaccinations is Not NULL
ORDER BY 1,2;

SELECT Location, date, population, people_vaccinated, people_fully_vaccinated, total_vaccinations
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is Not NULL
AND total_vaccinations is Not NULL
ORDER BY 1,2;


--FULLY VACCINATED PERCENTAGE

SELECT Location, date, population, people_vaccinated, people_fully_vaccinated, total_vaccinations, 
(people_fully_vaccinated/population)*100 as fully_vaccinated_percentage
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is Not NULL
AND total_vaccinations is Not NULL
ORDER BY 1,2;

--FULLY VACCINATED PERCENTAGE ACROSS THE GLOBE (PRESENT) //COUNTRIES

SELECT Location, population, MAX(people_fully_vaccinated) as people_fully_vaccinated,
MAX((people_fully_vaccinated/population))*100 as fully_vaccinated_percentage
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is Not NULL
AND total_vaccinations is Not NULL
GROUP BY location, population
ORDER BY 1,2;

--FULLY VACCINATED PERCENTAGE ACROSS THE GLOBE (PRESENT) //continents

SELECT Location, population, MAX(people_fully_vaccinated) as people_fully_vaccinated,
MAX((people_fully_vaccinated/population))*100 as fully_vaccinated_percentage
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is NULL
AND total_vaccinations is Not NULL
GROUP BY location, population
ORDER BY 1,2;


-- JOINING BOTH THE TABLES

SELECT * 
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
ON dea.location = vacc.location
AND dea.date = vacc.date;

--USING AGGREGATE FUNCTIONS TO CREATE ROLLING COUNT WITH NEW VACCINATIONS FOR TOTAL VACCINATIONS COLUMN AND ORDERING IT BY LOCATION AND DATE
-- USING CAST (USING INT WASN'T SUCESSFULL, CHANGED IT TO BIGINT)

SELECT dea.continent, dea.location, dea.date, dea.population, vacc.new_vaccinations,
SUM(CAST(vacc.new_vaccinations AS bigint)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as total_vaccinations
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
ON dea.location = vacc.location
AND dea.date = vacc.date
WHERE dea.continent is not null
ORDER BY 2,3;

-- USING CONVERT()

SELECT dea.continent, dea.location, dea.date, dea.population, vacc.new_vaccinations,
SUM(CONVERT(bigint,vacc.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_total_vacc
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
	ON dea.location = vacc.location
	AND dea.date = vacc.date
WHERE dea.continent is not null
ORDER BY 2,3;



--Creating CTE

WITH PopvsVacc (Continent, Location, Date, Population, New_vaccinations, Rolling_total_vacc) 
AS (
SELECT dea.continent, dea.location, dea.date, dea.population, vacc.new_vaccinations,
SUM(CONVERT(bigint,vacc.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_total_vacc
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
	ON dea.location = vacc.location
	AND dea.date = vacc.date
WHERE dea.continent is not null
--ORDER BY 2,3;
)
SELECT * FROM PopvsVacc;

--Percentage of rolling_total_vacc

WITH PopvsVacc (Continent, Location, Date, Population, New_vaccinations, Rolling_total_vacc) 
AS (
SELECT dea.continent, dea.location, dea.date, dea.population, vacc.new_vaccinations,
SUM(CONVERT(bigint,vacc.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_total_vacc
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
	ON dea.location = vacc.location
	AND dea.date = vacc.date
WHERE dea.continent is not null
--ORDER BY 2,3;
)
SELECT *, (Rolling_total_vacc/Population)*100 as Rolling_total_vacc_percentage
FROM PopvsVacc;


--Rolling_total_vacc_percentage FOR EVERY LOCATION ON TOTAL (WITH CTE)

WITH PopvsVacc (Continent, Location, Date, Population, New_vaccinations, Rolling_total_vacc) 
AS (
SELECT dea.continent, dea.location, dea.date, dea.population, vacc.new_vaccinations,
SUM(CONVERT(bigint,vacc.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_total_vacc
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
	ON dea.location = vacc.location
	AND dea.date = vacc.date
WHERE dea.continent is not null
--ORDER BY 2,3;
)
SELECT Location, Population, MAX(Rolling_total_vacc) as Rolling_total_vacc, MAX((Rolling_total_vacc/Population)*100) as Rolling_total_vacc_percentage
FROM PopvsVacc
GROUP BY Location, Population
ORDER BY 1;



--CREATING A TEMP TABLE

DROP TABLE IF EXISTS #PercentPopulationVaccinated
CREATE TABLE #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
Rolling_total_vacc numeric)

INSERT INTO #PercentPopulationVaccinated
SELECT dea.continent, dea.location, dea.date, dea.population, vacc.new_vaccinations,
SUM(CONVERT(bigint,vacc.new_vaccinations)) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_total_vacc
FROM Portfolioproject_01..covid_deaths$ as dea
JOIN Portfolioproject_01..Covid_vaccinations$ as vacc
	ON dea.location = vacc.location
	AND dea.date = vacc.date
WHERE dea.continent is not null
ORDER BY 2,3;

SELECT *, (Rolling_total_vacc/Population)*100 as Rolling_total_vacc_percentage
FROM #PercentPopulationVaccinated;


--USING COLUMNS LIKE TOTAL,NEW TESTS TO GET NEW INSIGHTS
--PERCENTAGE OF POSITIVE RESULTS IN EACH REPORTING COUNTRIES

SELECT continent, location, date, new_cases, total_cases, new_tests, MAX(total_tests)
FROM Portfolioproject_01..covid_deaths$
ORDER BY location,date;

SELECT location, MAX(total_cases) total_cases, MAX(total_tests) total_tests, 
(MAX(total_cases)/MAX(total_tests))*100 as Percentage_of_Positive_results
FROM Portfolioproject_01..covid_deaths$
WHERE continent is not null
GROUP BY location
ORDER BY location;


--CREATING VIEW FOR LATER DATA VISULATION

CREATE VIEW death_rate 
AS
SELECT Location, date, population, new_cases, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
WHERE continent is Not NULL
--ORDER BY 1,2;

SELECT * FROM death_rate;

CREATE VIEW death_rate_India as
SELECT Location, date, population, new_cases, total_cases, total_deaths, (total_deaths/total_cases)*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
WHERE location = 'India'
AND continent is Not NULL
--ORDER BY 1,2;


CREATE VIEW Percent_Infected_Worldwide as
SELECT Location, population, MAX(total_cases) AS total_cases, MAX((total_cases/population))*100 AS Percent_Infected
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is not null
GROUP BY Location, population
--ORDER BY Percent_Infected DESC;


CREATE VIEW Death_rate_worldwide as
SELECT Location, population, MAX(CAST(total_deaths AS int)) AS total_deaths, MAX((total_deaths/population))*100 AS Death_rate
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is Not NULL
GROUP BY Location, population
--ORDER BY Death_rate DESC;

CREATE VIEW Total_deathcount_Worldwide as
SELECT Location, MAX(CAST(total_deaths AS int)) AS total_deaths_count
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is Not NULL
GROUP BY Location
--ORDER BY total_deaths_count DESC;


CREATE VIEW Total_deathcount_Worldwide_Cont AS
SELECT location, MAX(CAST(total_deaths AS int)) AS total_deaths_count
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is NULL
GROUP BY location
--ORDER BY total_deaths_count DESC;


CREATE VIEW totaldeaths_world AS
SELECT date,SUM(new_cases) as total_cases, SUM(CAST(new_deaths as int)) AS total_deaths, 
(SUM(CAST(new_deaths as int))/SUM(new_cases))*100 AS death_rate
FROM Portfolioproject_01..covid_deaths$
--WHERE location = 'India'
WHERE continent is NOT NULL
GROUP BY date HAVING SUM(new_cases) IS NOT NULL
--ORDER BY 1,2;


CREATE VIEW fully_vaccinated_worldwide AS
SELECT Location, population, MAX(people_fully_vaccinated) as people_fully_vaccinated,
MAX((people_fully_vaccinated/population))*100 as fully_vaccinated_percentage
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is Not NULL
AND total_vaccinations is Not NULL
GROUP BY location, population
--ORDER BY 1,2;

CREATE VIEW fully_vaccinated_continents as
SELECT Location, population, MAX(people_fully_vaccinated) as people_fully_vaccinated,
MAX((people_fully_vaccinated/population))*100 as fully_vaccinated_percentage
FROM Portfolioproject_01..Covid_vaccinations$
--WHERE location = 'India'
WHERE continent is NULL
AND total_vaccinations is Not NULL
GROUP BY location, population
--ORDER BY 1,2;

CREATE VIEW total_tests_worldwide AS
SELECT location, MAX(total_cases) total_cases, MAX(total_tests) total_tests, 
(MAX(total_cases)/MAX(total_tests))*100 as Percentage_of_Positive_results
FROM Portfolioproject_01..covid_deaths$
WHERE continent is not null
GROUP BY location
--ORDER BY location;