import csv from 'csv-parser'
import fs from 'fs'
import { Pool, Client } from 'pg'

//Connection to postgress sql
const connectionString = 'postgresql://octopus:secret@localhost:5432/wtw-dev'
const client = new Client({
    connectionString: connectionString,
})

client.connect()

// creamos tabla si no existe
/*
const createTableText =
    `CREATE TABLE IF NOT EXISTS policies (
        clientId VARCHAR(255) NOT NULL,
        brokerShortName VARCHAR(255) NOT NULL,           
        department VARCHAR(255) NOT NULL,
        coverNumber INT NOT NULL,
        churnPropensityScore FLOAT NOT NULL,
        riskClassOnPreChurnPolicies	VARCHAR(255) NOT NULL,
        businessDescription	VARCHAR(255) NOT NULL,
        versionNumber INT NOT NULL,
        effectiveDate	DATE NOT NULL,
        expiryDate	DATE NOT NULL,
        premium	FLOAT NOT NULL,
        totalIncome	FLOAT NOT NULL,
        stampDudy	FLOAT NOT NULL,
        fireServicesLevy	FLOAT NOT NULL,
        insureCharge	INT NOT NULL,
        brokerCharge	FLOAT NOT NULL,
        brokerAdminFeeGST	FLOAT NOT NULL,
        adminFee	FLOAT NOT NULL,
        discount	FLOAT NOT NULL,
        gstOnDiscount	FLOAT NOT NULL,
        gstOnPremium	FLOAT NOT NULL,
        brokerage	FLOAT NOT NULL,
        gstOnBrokerage	FLOAT NOT NULL,
        dueByClient	FLOAT NOT NULL,
        paidToDate	FLOAT NOT NULL,
        subAgentIncome	FLOAT NOT NULL,
        thirdPartyBrokerRate	FLOAT NOT NULL,
        thirdPartyBrokerCommission	FLOAT NOT NULL,
        incomeClass	VARCHAR(255) NOT NULL,
        top1 VARCHAR(255) NOT NULL,
        top2 VARCHAR(255) NOT NULL,
        top3 VARCHAR(255) NOT NULL,
        top4 VARCHAR(255) NOT NULL,
        top5 VARCHAR(255) NOT NULL,
        top6 VARCHAR(255) NOT NULL,
        top7 VARCHAR(255) NOT NULL,
        top8 VARCHAR(255) NOT NULL,
        top9 VARCHAR(255) NOT NULL,
        top10 VARCHAR(255) NOT NULL        
    )`;

client.query(createTableText)
    .then((res) => {
        console.log(res);
    })
    .catch((err) => {
        console.log(err);
        client.end();
    });

*/

// Read file 
const results = []
fs.createReadStream('top_recommended_riskclass_high_prechurn_august_w1.csv')
    .pipe(csv())
    .on('data', (data) => results.push(data))
    .on('end', () => {
        console.log(results);
        //save DataFile in DB
        const queryText = `INSERT INTO policies("clientId", "brokerShortName","department","coverNumber","churnPropensityScore","riskClass",
            "businessDescription","versionNumber","effectiveDate","expiryDate","premium","totalIncome","stampDuty","fireServicesLevy","insurerCharge",
            "brokerCharge","brokerAdminFeeGST","adminFee","discount","gstOnDiscount","gstOnPremium","brokerage","gstOnBrokerage","dueByClient","paidToDate",
            "subAgentIncome","thirdPartyBrokerRate","thirdPartyBrokerCommission","incomeClass","top1","top2","top3","top4","top5","top6","top7","top8","top9","top10")
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,
            $33,$34,$35,$36,$37,$38,$39) RETURNING id`

        results.map((row) => (
            client.query(queryText, [
                row.ClientId,
                row.Broker,
                row.Department,
                row.CoverNumber,
                row.ChurnPropensityScore,
                row.RiskClassonPreChurnPolicies,
                row.BusinessDescription,
                row.VersionNumber,
                row.EffectiveDate,
                row.ExpiryDate,
                row.Premium,
                row.TotalIncome,
                row.StampDuty,
                row.FireServicesLevy,
                row.InsurerCharge,
                row.BrokerCharge,
                row.BrokerAdminFeeGST,
                row.AdminFee,
                row.Discount,
                row.GSTonDiscount,
                row.GSTonPremium,
                row.Brokerage,
                row.GSTonBrokerage,
                row.DuebyClient,
                row.PaidtoDate,
                row.SubAgentIncome,
                row.ThirdPartyBrokerRate,
                row.ThirdPartyBrokerCommission,
                row.IncomeClass,
                row.Top_1,
                row.Top_2,
                row.Top_3,
                row.Top_4,
                row.Top_5,
                row.Top_6,
                row.Top_7,
                row.Top_8,
                row.Top_9,
                row.Top_10,
            ], (err, res) => {
                console.log(err, res)
            })
        ));
    });



