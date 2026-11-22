# v13 — Binance Demo signed `clientAlgoId` lookup for v12 fence recovery

## Change

* `exchange.Client.QueryDemoAlgoByClientID` adds a **GET-only** USD-M Futures
  `/fapi/v1/algoOrder?clientAlgoId=...` signed USER_DATA query. It uses the
  existing Demo-only `demoSignedRequest` (timestamp, recvWindow, HMAC, no
  redirects), decodes/validates an identified BTCUSDT conditional Algo and
  rejects invalid IDs, missing closePosition/reduceOnly fields, wrong modes/types, and unknown states. No POST/DELETE
  operation is introduced.
* `trading.productFenceSignedAlgoReader` provides the v12
  `protectionfence.SignedAlgoReader` interface, using stored credentials and
  `sha256(APIKey)`-matched account fingerprint. It checks exact clientAlgoId,
  plan instruction, side, working type, trigger price, quantity, exchange ID,
  and disallows closePosition/reduceOnly. It reports Binance -2013 as
  `Found=false` **without granting absence proof, retry or release**.
* `DemoProtectionRuntimeCoordinator.InspectProductMutationFenceSignedOnce`
  requires an existing exclusive coordinator lease, exactly one valid fence,
  an immutable original protection plan, and matching Demo credentials before
  any network request. Two signed GETs are executed by the v12 observer, which
  still returns all authorization flags false and never deletes/renames the
  fence. The plan is rechecked afterwards. No HTTP route or automation loop
  calls this method.

## Security and operational boundaries

1. A signed *request* authenticates with API-key HMAC but the transport's
   response is not itself a cryptographic signature. Two identical results
   neither establish continuous account ownership nor preclude fills between
   reads. Binance can return `-2013` for expired, missing or temporarily
   unqueryable orders; it is **not proof of non-submission**.
2. The existing v11 global mutation fence has **no safe release protocol**.
   Neither this adapter nor v12 permits re-submission, sibling cancellation,
   settlement, or any order mutation.
3. A plan is compared with the saved local plan; no local/remote plan mismatch
   or changed API-key identity is accepted. Strings for the observed quantity
   remain strictly equal under the v12 classifier, so harmless formatting
   differences can intentionally trigger manual review.
4. Tests use only loopback HTTP mocks and synthetic copies of the relevant
   `exchange` and `trading` baseline signatures. These tests **cannot** certify
   compilation or runtime behavior of the entire GitHub `738d628` tree.
5. No real Binance key, PC account, trade, network order, HTTP START unlock,
   or financial COMMIT is used or authorized here. Product `Arm` and
   `CancelTriggeredSibling` retain `ErrProtectionApproval`; product START
   remains HTTP 423 until a separate signed ownership and Demo E2E review.
6. API specifics must be reverified in the controlled Demo environment. The
   live Demo GET is *not* tested here. Signed reads may consume request quotas.
7. `protectionfence.Open` creates a directory, so the product method checks
   that the fence directory already exists before opening it. The method does
   not deliberately create an empty fence inventory.

## Next

* Independently verify signed account-wide Hedge positions and all ownership,
  including other models/manual trades and partial/double trigger outcomes.
* Implement an explicit multi-stage, crash-safe *non-replay* recovery ledger
  with independent operator approval; never auto-release an uncertain fence.
* Test real Binance Demo authenticated GET behavior and all whole-repo Go
  tests, Windows durability, restart behavior before any execution authority.
