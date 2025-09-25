PS C:\Users\abdul\OneDrive\Desktop\RPC-fetcher\RPC> node ./CloudDB_IngestionScript.js `
>>   --rpc "https://public-zigchain-testnet-rpc.numia.xyz/block_results?height=" `
>>   --blockRpc "https://public-zigchain-testnet-rpc.numia.xyz/block?height=" `
>>   --from 2076238 `
>>   --to 2096238 `
>>   --pg "postgresql://postgres:Masamuna1$@experimentalfinal.postgres.database.azure.com:5432/postgres?sslmode=require" `
>>   --concurrency 2 `
>>   --workers 2
