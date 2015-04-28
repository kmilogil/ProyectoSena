<%-- 
    Document   : cargarPedido
    Created on : 2/04/2015, 12:31:54 AM
    Author     : User
--%>

<%@page import="modulo.ofertas.dto.OfertaDto"%>
<%@page import="modulo.ofertas.FOferta"%>
<%@page contentType="text/html" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"> 
        <script type="text/javascript" src="../js/jquery-1.11.2.js"></script>
        <script type="text/javascript" src="../js/bootstrap.js"></script>
    </head>
    <body>
        <%
            FOferta faOfer = new FOferta();
            if (request.getParameter("idOferta") != null) {

                OfertaDto ofDto = faOfer.obtenerOfertaPorId(Integer.parseInt(request.getParameter("idOferta")));
                String cantidad = Long.toString(ofDto.getInDto().getCantidad());

        %>

        <strong><%=" " + ofDto.getPreDto().getDescripcion()%> de </strong> (<%=ofDto.getProAso().getProDto().getNombres()%>)<strong> disponibles </strong><strong id="cantidad"><%=cantidad%></strong> 
        <input type="hidden" name="idOfer" id="idOferta" value="<%=ofDto.getIdOferta()%>">
        <%
        } else if (request.getParameter("idOfer") != null) {
            OfertaDto ofDto = faOfer.obtenerOfertaPorId(Integer.parseInt(request.getParameter("idOfer")));
            if (ofDto.getIdOferta() != 0) {


        %>


        <h4 class="text-center text-capitalize" id="productoOferta"><strong><%=ofDto.getProAso().getProDto().getNombres()%></strong></h4>
        <br>
        <div class="col-md-12">
            <div class="form-group" id="abCantidad">
                <label class="control-label" for="aCantidad">Cantidad de abastecimiento: </label>
                <input type="text" class="form-control" id="aCantidad" onblur="validarAbastecimientoPro(this);" onkeypress="return justNumbers(event);" value=""
                       name="aCantidad" maxlength="10">                
            </div>                                                    
            <center>
                <input type="hidden" name="idOfert" id="idOfert" value="<%=ofDto.getIdOferta()%>">
                <input type="submit" name="abastecerOferta" id="abastecerOferta" class="btn btn-success" value="Abastecer producto" disabled="true">                                                        
            </center>
            <br>
        </div> 
        <%

        } else {

        %>
        <h4 class="text-center"><strong>Esta oferta se encuentra vencida</strong></h4>
        <%                }
            }
        %>
    </body>
</html>
