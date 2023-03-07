
var serverhost = 'http://localhost:8000';


const interval = setInterval(() => {
    const header = document.querySelector("._1d1--0DMy_jAIxCCoYMo1k ");
    if (header) {
        clearInterval(interval)

        const button = document.createElement("button")
        button.innerHTML = "aperte"
        button.classList.add("botao")
        let texto
        //(texto).val('');
        button.addEventListener("click",()=>{
            let texto = document.querySelector("div.DraftEditor-editorContainer div div div div div span span").textContent;
            //.textContent;
            console.log(texto);
            var search_topic = texto
            console.log(search_topic)
            if (search_topic){
                console.log(texto)
                chrome.runtime.sendMessage(
                {contentScriptQuery: 'get_wiki_summary', textid: search_topic},
                    );
            }
        })

        
        header.appendChild(button)
    }
},
1000);


