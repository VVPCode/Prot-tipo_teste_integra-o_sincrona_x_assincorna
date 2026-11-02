// Mensagem de pedido com a estrutura do Quadro 3: identificador em UUID,
// inteiro, data e hora com fuso, texto enumerado, valor logico, decimal, texto
// livre e tres itens. O tamanho e ajustado ao nivel do experimento apenas pelo
// comprimento do campo de observacao, de modo que o numero de linhas gravadas
// por operacao seja o mesmo nos dois niveis (Quadro 5).

const CANAIS = ['site', 'aplicativo', 'loja'];

// Nomes curtos de proposito: com descricoes longas o pedido serializado parte de
// 562 bytes, e o nivel de 500 bytes do Quadro 5 nao seria alcancavel. Assim o
// piso fica em cerca de 477 bytes e o campo de observacao cobre a diferenca nos
// dois niveis, como o texto exige.
const DESCRICOES = ['Fone sem fio', 'Teclado compacto', 'Suporte de monitor'];

function hex(valor, digitos) {
  const texto = Math.abs(Math.trunc(valor)).toString(16);
  return ('0'.repeat(digitos) + texto).slice(-digitos);
}

// UUID valido e unico dentro da execucao, derivado do usuario virtual e da
// iteracao. Nao usa Math.random porque usuarios virtuais do k6 podem partir da
// mesma semente, e uma colisao seria gravada como reentrega pela chave primaria.
export function uuidPedido(semente, vu, iteracao) {
  return [
    hex(semente, 8),
    hex(vu, 4),
    '4' + hex(iteracao >> 12, 3),
    '8' + hex(vu ^ 0x7ff, 3),
    hex(iteracao, 12),
  ].join('-');
}

export function montarPedido(tamanhoAlvo, vu, iteracao, semente) {
  const itens = [
    { sku: 'SKU-0001', descricao: DESCRICOES[0], quantidade: 1, preco_unitario: 199.9 },
    { sku: 'SKU-0002', descricao: DESCRICOES[1], quantidade: 2, preco_unitario: 345.5 },
    { sku: 'SKU-0003', descricao: DESCRICOES[2], quantidade: 3, preco_unitario: 72.25 },
  ];
  // Quadro 3: valor_total igual a soma de quantidade x preco unitario.
  const total = itens.reduce((acc, i) => acc + i.quantidade * i.preco_unitario, 0);

  const pedido = {
    pedido_id: uuidPedido(semente, vu, iteracao),
    cliente_id: 1000 + (iteracao % 9000),
    // Alguns milissegundos no passado: o Quadro 3 exige que nao seja posterior
    // ao instante de recebimento pelo produtor.
    data_hora_pedido: new Date(Date.now() - 50).toISOString(),
    canal_venda: CANAIS[(vu + iteracao) % CANAIS.length],
    entrega_expressa: (vu + iteracao) % 2 === 0,
    valor_total: Math.round(total * 100) / 100,
    observacao: '',
    itens: itens,
  };

  const atual = JSON.stringify(pedido).length;
  const falta = tamanhoAlvo - atual;
  if (falta > 0) {
    pedido.observacao = 'p'.repeat(falta);
  }
  return JSON.stringify(pedido);
}


// Tamanho do pedido sem preenchimento, usado para conferir na partida se o
// nivel pedido e alcancavel.
export function pisoDoPedido() {
  const amostra = montarPedido(0, 1, 1, 0);
  return amostra.length;
}
