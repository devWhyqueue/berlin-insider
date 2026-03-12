
- After every code change, you need to run clean code skill with option --minimal and pass it with no violations.

- You can take a look at the deployment with command:

Windows:

```
ssh ubuntu@89.168.90.195 -i C:\Users\yanni\.ssh\ssh-key-2023-09-20.key
```

In WSL you can omit the -i argument.

- Never change code on the server, treat as readonly. You may make data changes to the database, however.