FROM alpine
RUN apk update && apk add ca-certificates
ADD main /
CMD ["/main"]
