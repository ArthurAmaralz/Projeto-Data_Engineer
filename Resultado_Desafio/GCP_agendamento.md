 ### 5. Faça uma rotina que mensalmente colete os dados do ultimo mes e adiciona apenas os dados que sejam novos. Essa rotina deve rodar automaticamente todos os meses, escolha a forma que preferir para essa atividade. `Rotina concluida com soluções tecnicas alternativas, porpulamente conhecida por Gambiarra`
 
A ideia a principio seria ultilizar o cloud schedute, cloud functions e pub/sub para realizar a trigger entre eles, e persistir os dados no google stored como no esquema a baixo:
![1_M_hAvJZVNbKTy86VOlK4QA](https://user-images.githubusercontent.com/61892694/198929868-44c8a80c-5594-4ce6-a382-dc7aac02a243.png)

![2022-10-30_21h09_42](https://user-images.githubusercontent.com/61892694/198928607-bd50a9a1-a99c-4b24-9f42-7d2cb59d620b.png)
