/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================

🇬🇧 ENGLISH:
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;

===============================================================================
🇫🇷FRANÇAIS
Objectif du script :

	Cette procédure stockée effectue le processus ETL (Extraire, Transformer, Charger) pour
	Remplir les tableaux de schéma « argent » à partir du schéma « bronze ».
	Actions effectuées:
		- Tronce les tables d'argent.
		- Insère des données transformées et nettoyées de Bronze dans des tableaux Silver.

Paramètres :
	Aucun.
		Cette procédure stockée n'accepte aucun paramètre ni ne renvoie aucune valeur.

Exemple d'utilisation :
	EXEC Silver.load_silver ;

⚠️ IMPORTANT

1- Au niveau de la Table silver.crm_prd_info:

  Nous avons fait quelques modifications en ajoutant une Table (cat_id, type NVARCHAR) et 
  en modifiant la DataType de 2 tables:
      prd_start_dt  type DATETIME en type DATE
      prd_end_dt    type DATETIME en type DATE

  Explication: Ces modifications sont dues à cause de la qualité de la data

  En effet au niveau de la colonne 'prd_key' nous avons derivé une nouvelle colonne en 
  creant la colonne 'cat_id' basé sur les calculs ou les transformations d'une colonne
  qui existe deja (prd_key). 
    Parce que parfois nous avons besoin que des colonnes pour l'analyse et nous ne
    pouvons pas à chaque fois aller à la source et demander de les creer. Raison pour
    nous derivons nos propres colonnes que nous voulons pour les analyses


2- Au niveau de la Colonne prd_cost : ISNULL (prd_cost, 0) AS prd_cost,
  En utilisant la commande ISNULL, nous remplaçons par 0 si la valeur est NULL
  sur toutes les lignes de la cette colonne (la Standarisation).
    Paerce que nous sommes supposé avoir des INT dans la cette colonne

3- Au niveau de la Colonne prd_start_dt:
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt

  Data Enrichment: Le but est d'ajoute ou une donner une valeur à notre data.

    Le but de l'operation ici etait de regler le probleme qu'il y avait avec les dates
    prd_start_dt et prd_end_dt

      * La start date de la prochaine ligne doit etre la End date de la ligne
        precedente:
            prd_id    prd_start_dt    prd_end_dt
              23       2010-08-11     2010-08-12
              24       2010-08-12     2010-08-27
              25       2010-08-27      NULL
      ☠️ Cette operation doit etre executé si il y'a une incoherence au
          des dates dans la data, ou les dates ne match pas

4- Au niveau de la Table silver.crm_sales_details, nous avons procedé aux transformations suivantes:
		
	On a eu à faire des transformations suivantes

	1️⃣ * Normalisation: Nous avons normaliser la date dans les colonnes
		(sls_order_dt, sls_ship_dt, sls_due_dt) en disant si la valeur dans
		dans les colonnes valent 0 ou si la longuer des valeurs est differentes
		de 8 caracteres; alors nous aurons NULL (Valeur manquante)
							>>> voir indince A

		* Convertion & Transformation: Pour les valeurs valides (8) caracters;
		Nous avons en premier temps convertis les valeurs INT dans les colonnes
		(sls_order_dt, sls_ship_dt, sls_due_dt) en VARCHAR, ensuite nous les avons
		transformées en format DATE  >>> voir indice B

	Le Script:
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL >>> Indice A
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) >>> Indice B
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,


	2️⃣* Transformation: Correction du montant de ventes
		
		En utilisant une règle en Business qui dit: Σ sales= Quantity x Price. Et que
		toute la quantité des ventes (sales quantity) et les info des Prix doivent être
		des nombres positifs: ❌ Negative, zeros, NULL are Not Allowed !

		Nous avons procedé de la maniere suivante dans ces colonnes:

		⭐️Cas de la colonne sls_sales:

			La logique:
				si sls_sales est NULL (manquant) ou
				si sls_sales est <= 0 (invilide) ou
				si sls_sales ne correspond pas à la formule Quantité x Prix (incoherent)

			Alors nous aurons: 
				Qunad une des conditions est vraie --> Calcule du montant correct avec
					sls_quantity x ABS(sls_price).
				>>> ABS(sls_sales)  ici garantit un prix positif (même si la valeur originale
					étais négative)

			Ensuite conservation de la valeur originale:
				Si aucune condition n'est vraie --> On garde la valeur existante de sls_sales

Le Script:
		CASE 
		    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
		        THEN sls_quantity * ABS(sls_price)
		    ELSE sls_sales
		END AS sls_sales


		⭐️Cas decla colonne sls_price: Correction du prix unitaire

			La logique:
				Conditions pour recalculer :
					Si sls_price est NULL ou
					Si sls_price est ≤ 0 (invalide)
				
				Recalcul :
					Calcule le prix avec sls_sales / sls_quantity
					NULLIF(sls_quantity, 0) évite une division par 0 (retourne NULL si quantité=0)
				
				Conservation de la valeur originale :
					Si le prix est valide (>0) → Garde la valeur existante

Le Script:	
		CASE 
		    WHEN sls_price IS NULL OR sls_price <= 0 
		        THEN sls_sales / NULLIF(sls_quantity, 0)
		    ELSE sls_price
		END AS sls_price
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		-- Loading silver.crm_cust_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer
		SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		-- Loading silver.crm_prd_info
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL (prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading crm_sales_details
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) 
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price  -- Derive price if original value is invalid
			END AS sls_price
		FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

        -- Loading erp_cust_az12
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
		)
		SELECT
			CASE
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) -- Remove 'NAS' prefix if present
				ELSE cid
			END AS cid, 
			CASE
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END AS bdate, -- Set future birthdates to NULL
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				ELSE 'n/a'
			END AS gen -- Normalize gender values and handle unknown cases
		FROM bronze.erp_cust_az12;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

        -- Loading erp_loc_a101
        SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
		)
		SELECT
			REPLACE(cid, '-', '') AS cid, -- Gerer les valeurs invalides en remplaçant '-' par rien ' ' dans la colonne cid
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END AS cntry -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
	    SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
		
		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
