 ### 5. Faça uma rotina que mensalmente colete os dados do ultimo mes e adiciona apenas os dados que sejam novos. Essa rotina deve rodar automaticamente todos os meses, escolha a forma que preferir para essa atividade. </br>`Rotina concluida com soluções tecnicas alternativas, porpulamente conhecida por Gambiarra`
 
 
 
A ideia a principio seria importar o arquivo .py e ultilizar o cloud schedute, cloud functions e pub/sub para realizar a trigger entre eles, e persistir os dados no google stored como no esquema a baixo:


![1_M_hAvJZVNbKTy86VOlK4QA](https://user-images.githubusercontent.com/61892694/198929868-44c8a80c-5594-4ce6-a382-dc7aac02a243.png)


#### **Configurando o google scheduler:**
![2022-10-30_19h39_08](https://user-images.githubusercontent.com/61892694/198930541-2e459df2-69f6-4608-a0f9-73ebc99be63d.png)

![2022-10-30_14539_08](https://user-images.githubusercontent.com/61892694/198931640-3014a5f6-217b-4e8e-9b28-335aa21be500.png)
#### Ajustando o Pub/sub:
![2022-10-30_19h53_09](https://user-images.githubusercontent.com/61892694/198931939-38607e96-a547-4b0d-872a-fd44a31a6062.png)

#### **Mas ao finalizar no google functions tive o seguinte erro, onde mesmo com o 'main.py' e até definindo a função não consegui executar**
![2022-10-30_21h09_42](https://user-images.githubusercontent.com/61892694/198928607-bd50a9a1-a99c-4b24-9f42-7d2cb59d620b.png)


### Então por esse motivo optei pela solução contida em:

- [Solução tecnica alternativa para schedule](Resultado_Desafio/Teste_agendamento_altenativo.ipynb)
