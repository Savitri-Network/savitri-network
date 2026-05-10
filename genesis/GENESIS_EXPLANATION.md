# Genesis Block Configuration — Savitri Network

## Detailed structure

The `genesis.json` file implements the initial distribution of SAVI tokens
according to the tokenomics defined in this codebase.

## Total supply: 230M SAVI tokens

### Vesting wallets (locked) — 220M SAVI

#### 1. Team Vesting Allocation (20M tokens)
- **Address**: `0x000…0001`
- **Amount**: 20,000,000 SAVI (20 × 10¹⁸ wei)
- **Vesting**: 4 years with a 1-year cliff
- **Availability**: 0 immediately, gradual unlock after 1 year
- **Purpose**: Reward for the development team

#### 2. Investors Allocation (50M tokens)
- **Address**: `0x000…0002`
- **Amount**: 50,000,000 SAVI (50 × 10¹⁸ wei)
- **Vesting**: 2 years with a 6-month cliff
- **Availability**: 0 immediately, gradual unlock after 6 months
- **Purpose**: Allocation for early investors

#### 3. Foundation Allocation (30M tokens)
- **Address**: `0x000…0003`
- **Amount**: 30,000,000 SAVI (30 × 10¹⁸ wei)
- **Vesting**: 10 years with a 1-year cliff
- **Availability**: 0 immediately, gradual unlock after 1 year
- **Purpose**: Foundation funds and ecosystem development

#### 4. Community Rewards Allocation (120M tokens)
- **Address**: `0x000…0004`
- **Amount**: 120,000,000 SAVI (120 × 10¹⁸ wei)
- **Vesting**: 8 years with a 3-month cliff
- **Availability**: 0 immediately, gradual unlock after 3 months
- **Purpose**: Community rewards, staking, mining

### Immediately available wallets — 10M SAVI

#### 5. Treasury Wallet (5M tokens)
- **Address**: `0x000…0005`
- **Amount**: 5,000,000 SAVI (5 × 10¹⁸ wei)
- **Availability**: 5,000,000 SAVI immediately available
- **Purpose**: Treasury operations, liquidity, emergencies
- **Control**: Multisig with 3-of-5 signatures required

#### 6. Development Fund (3M tokens)
- **Address**: `0x000…0006`
- **Amount**: 3,000,000 SAVI (3 × 10¹⁸ wei)
- **Availability**: 3,000,000 SAVI immediately available
- **Purpose**: Continuous development, bug bounties, grants
- **Control**: Technical team with governance approval

#### 7. Marketing & Ecosystem Fund (2M tokens)
- **Address**: `0x000…0007`
- **Amount**: 2,000,000 SAVI (2 × 10¹⁸ wei)
- **Availability**: 2,000,000 SAVI immediately available
- **Purpose**: Marketing, partnerships, adoption
- **Control**: Marketing team with quarterly budget

## Distribution summary

| Category | Wallet | Amount | Available at genesis | Vesting |
|---|---|---|---|---|
| Team | 0x000…0001 | 20M SAVI | 0 (1-year cliff) | 4 years |
| Investors | 0x000…0002 | 50M SAVI | 0 (6-month cliff) | 2 years |
| Foundation | 0x000…0003 | 30M SAVI | 0 (1-year cliff) | 10 years |
| Community | 0x000…0004 | 120M SAVI | 0 (3-month cliff) | 8 years |
| Treasury | 0x000…0005 | 5M SAVI | **5M SAVI** | No |
| Development | 0x000…0006 | 3M SAVI | **3M SAVI** | No |
| Marketing | 0x000…0007 | 2M SAVI | **2M SAVI** | No |
| **TOTAL** | **7 wallets** | **230M SAVI** | **10M SAVI (4.3%)** | **220M SAVI (95.7%)** |

## Circulating supply at genesis

- **Total supply**: 230,000,000 SAVI
- **Immediately circulating**: 10,000,000 SAVI (4.3%)
- **Locked in vesting**: 220,000,000 SAVI (95.7%)

## Wallet configuration

### Private keys to generate

For production, generate the following Ed25519 private keys:

1. `0x000…0001` — Team vesting wallet
2. `0x000…0002` — Investors vesting wallet
3. `0x000…0003` — Foundation vesting wallet
4. `0x000…0004` — Community vesting wallet
5. `0x000…0005` — Treasury multisig wallet
6. `0x000…0006` — Development fund wallet
7. `0x000…0007` — Marketing fund wallet

### Wallet security

- **Treasury**: 3-of-5 multisig with separate keyholders
- **Development**: Hardware wallet plus governance approval
- **Marketing**: Hot wallet with quarterly key rotation
- **Vesting**: Cold storage with scheduled access

## Initialization process

1. **Load genesis block**: `load_genesis_block()` parses this JSON.
2. **Initialize vesting**: `initialize_genesis_mint()` creates the vesting schedules for wallets 1–4.
3. **Initialize available supply**: wallets 5–7 receive immediately available tokens.
4. **Store block**: the genesis block is persisted to storage.
5. **Initialize accounts**: accounts are created with their initial balances.
6. **Set up supply**: the SupplyManager is initialized with the 230M total supply.

## Production notes

### Replace placeholder addresses

- Replace each `0x000…000X` with a real, generated address.
- Generate secure Ed25519 private keys for every wallet.
- Implement multisig for the treasury wallet.

### Security configuration

- Cold storage for vesting wallets.
- Multisig for the treasury (3-of-5 signatures).
- Hardware wallet for the development fund.
- Regular rotation for the marketing wallet.

### Governance

- Proposals for development / marketing fund spending.
- Quarterly fund-utilization reports.
- Annual audit of vesting wallets.

## Tokenomics integration

This genesis block is consistent with:

- **Dynamic burn rate**: 0.1% – 1% based on 24-hour volume.
- **Halving system**: every 2 years for the first 20 years, then every 5 years.
- **Supply tracking**: persistent mint / burn in storage.
- **Governance**: token-weighted voting on available balances.
- **Staking rewards**: streamed from the 120M community vesting over time.

## Supply timeline

- **Genesis**: 10M SAVI available (4.3%).
- **3 months**: community rewards begin unlocking (+~3.75M).
- **6 months**: investor unlocks begin (+~2.08M).
- **1 year**: team and foundation unlocks begin (+~5M).
- **2 years**: end of investor vesting (+~25M total).
- **4 years**: end of team vesting (+~20M total).
- **8 years**: end of community vesting (+~120M total).
- **10 years**: end of foundation vesting (+~30M total).

This design provides initial liquidity for operations while protecting
long-term value through gradual vesting.
