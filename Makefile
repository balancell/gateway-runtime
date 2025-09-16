ifeq	($(age),5)
	UserName = "Gift hard"
else	
	UserName = "Nkosinathi Shesana"
endif

$(info	Lets print random shit here the var is ${UserName}) 
$$(UserName)
runApp:
	docker compose build
	docker compose up

runEcho:
	@echo "${UserName}" #@ so that the command is not printed on terminal [make runEcho UserName='Name Chosen']