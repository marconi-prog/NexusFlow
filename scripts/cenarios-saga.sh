#!/usr/bin/env bash
# Roda os 7 cenários da saga de pedido contra a API (precisa de API + workers + Camunda + Postgres).
# Uso: NEXUSFLOW_API_KEY=sua-chave ./scripts/cenarios-saga.sh   (NEXUSFLOW_URL padrão: http://localhost:8080)
# Pré-requisito: deploy de processes/bpmn/pedido-saga.bpmn. O estoque é alterado de verdade.
set -u
API="${NEXUSFLOW_URL:-http://localhost:8080}/api/v1"
H=(-H "X-API-Key: ${NEXUSFLOW_API_KEY:?defina NEXUSFLOW_API_KEY}" -H "Content-Type: application/json")

pedido() { curl -s -X POST "$API/orders" "${H[@]}" -d "$1" | python3 -c 'import sys,json; print(json.load(sys.stdin)["orderId"])'; }
status() { curl -s "$API/orders/$1" "${H[@]}" | python3 -c 'import sys,json; d=json.load(sys.stdin); o=d.get("order") or {"status":"PENDENTE"}; p=d.get("payment") or {}; r=d.get("reservation") or {}; print(o["status"], "|", o.get("rejectionReason",""), "| pagamento:", p.get("status","-"), "| reserva:", r.get("status","-"))'; }
espera() { # espera o pedido $1 chegar no status $2 (até 30s)
  for _ in $(seq 1 60); do s=$(status "$1"); [[ "$s" == "$2"* ]] && { echo "  $s"; return 0; }; sleep 0.5; done
  echo "  FALHOU: esperava $2, está: $s"; return 1
}
estoque() { curl -s "$API/inventory" "${H[@]}" | python3 -c 'import sys,json; print({i["sku"]: i["available"] for i in json.load(sys.stdin)["items"]})'; }
tarefa() { # chave da user task do pedido $1
  pik=$(curl -s "$API/orders/$1" "${H[@]}" | python3 -c 'import sys,json; print(json.load(sys.stdin)["order"]["processInstanceKey"])')
  for _ in $(seq 1 40); do
    k=$(curl -s "$API/user-tasks?processInstanceKey=$pik" "${H[@]}" | python3 -c 'import sys,json; it=json.load(sys.stdin)["items"]; print(it[0]["userTaskKey"] if it else "")')
    [ -n "$k" ] && { echo "$k"; return; }; sleep 0.5
  done
}

echo "estoque inicial: $(estoque)"

echo "1) caminho feliz: R\$ 250, entrega confirmada"
P=$(pedido '{"customer":"Ana","items":[{"sku":"MOUSE","quantity":2,"unitPrice":125}],"cardNumber":"4111111111111111"}')
espera "$P" "PAID"
curl -s -X POST "$API/orders/$P/delivery-confirmation" "${H[@]}" >/dev/null
espera "$P" "COMPLETED"

echo "2) estoque insuficiente (erro BPMN ESTOQUE_INSUFICIENTE)"
P=$(pedido '{"customer":"Bruno","items":[{"sku":"MONITOR","quantity":50,"unitPrice":10}],"cardNumber":"4111111111111111"}')
espera "$P" "REJECTED"

echo "3) cartão recusado (0002): compensação libera o estoque"
P=$(pedido '{"customer":"Carla","items":[{"sku":"TECLADO","quantity":3,"unitPrice":100}],"cardNumber":"4000000000000002"}')
espera "$P" "REJECTED | pagamento recusado: cartão recusado pelo emissor | pagamento: DECLINED | reserva: RELEASED"

echo "4) gateway instável (0003): 2 falhas técnicas, retries com backoff, depois aprova"
P=$(pedido '{"customer":"Davi","items":[{"sku":"MOUSE","quantity":1,"unitPrice":50}],"cardNumber":"4000000000000003"}')
espera "$P" "PAID"

echo "5) acima de R\$ 1000: aprovação humana REPROVADA"
P=$(pedido '{"customer":"Eva","items":[{"sku":"NOTEBOOK","quantity":1,"unitPrice":4500}],"cardNumber":"4111111111111111"}')
K=$(tarefa "$P"); echo "  user task $K"
curl -s -X POST "$API/user-tasks/$K/completion" "${H[@]}" -d '{"variables":{"aprovado":false}}' -o /dev/null -w "  completion HTTP %{http_code}\n"
espera "$P" "REJECTED | reprovado na aprovação manual | pagamento: - | reserva: RELEASED"

echo "6) acima de R\$ 1000 aprovado, mas prazo de entrega (10s) vence: estorno + liberação"
P=$(pedido '{"customer":"Fábio","items":[{"sku":"NOTEBOOK","quantity":1,"unitPrice":4500}],"cardNumber":"4111111111111111","deliveryDeadline":"PT10S"}')
K=$(tarefa "$P"); echo "  user task $K"
curl -s -X POST "$API/user-tasks/$K/completion" "${H[@]}" -d '{"variables":{"aprovado":true}}' -o /dev/null -w "  completion HTTP %{http_code}\n"
espera "$P" "PAID"
espera "$P" "REJECTED | prazo de entrega vencido | pagamento: REFUNDED | reserva: RELEASED"

INC_ID="PED-INC-$RANDOM"
echo "7) incidente: quantidade inválida -> corrigir variável -> resolver"
PIK=$(curl -s -X POST "$API/process-instances" "${H[@]}" -d '{"processDefinitionId":"pedido-saga","businessKey":"'$INC_ID'","variables":{"pedidoId":"'$INC_ID'","cliente":"Gil","itens":[{"sku":"MOUSE","quantidade":0,"precoUnitario":10}],"cartaoFinal":"1111","prazoEntrega":"PT1M"}}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["processInstanceKey"])')
for _ in $(seq 1 40); do
  INC=$(curl -s "$API/incidents?processInstanceKey=$PIK" "${H[@]}" | python3 -c 'import sys,json; it=json.load(sys.stdin)["items"]; print(it[0]["incidentKey"]+" "+it[0]["errorType"] if it else "")')
  [ -n "$INC" ] && break; sleep 0.5
done
echo "  incidente: $INC"
curl -s -X PUT "$API/process-instances/$PIK/variables" "${H[@]}" -d '{"variables":{"itens":[{"sku":"MOUSE","quantidade":1,"precoUnitario":10}]}}' -o /dev/null -w "  set variables HTTP %{http_code}\n"
curl -s -X POST "$API/incidents/${INC%% *}/resolution" "${H[@]}" -d '{"retries":1}' -o /dev/null -w "  resolution HTTP %{http_code}\n"
espera "$INC_ID" "PAID"

echo "estoque final: $(estoque)"
echo "eventos na auditoria:"
curl -s "$API/monitoring/jobs?window=1h" "${H[@]}" | python3 -c 'import sys,json; [print("  ", j["jobType"], "ok=",j["completed"], "falhas=",j["failed"], "erros BPMN=",j["errorsThrown"]) for j in json.load(sys.stdin)["items"]]'
