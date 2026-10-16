FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src
COPY src/TaskApi/TaskApi.csproj src/TaskApi/
RUN dotnet restore src/TaskApi/TaskApi.csproj
COPY src/TaskApi/ src/TaskApi/
RUN dotnet publish src/TaskApi/TaskApi.csproj -c Release --no-restore -o /app /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "TaskApi.dll"]
