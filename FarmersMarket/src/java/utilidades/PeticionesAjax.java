/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package utilidades;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import modulo.ofertas.FOferta;
import modulo.ofertas.dto.OfertaDto;
import modulo.pedidos.FPedido;
import modulo.usuarios.FUsuario;
import modulo.usuarios.dao.UsuarioDao;

/**
 *
 * @author Akashyro
 */
public class PeticionesAjax extends HttpServlet {

    /**
     * Processes requests for both HTTP <code>GET</code> and <code>POST</code>
     * methods.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        response.setContentType("text/html;charset=UTF-8");
        FUsuario faUsu = new FUsuario();
        //Verificación de la existencia de un documento
        if ((request.getParameter("idUsuario") != null)) {
            StringBuilder respuesta = new StringBuilder("");

            respuesta.append(faUsu.validarExistenciaDocumento(Long.parseLong(request.getParameter("idUsuario").trim())));

            PrintWriter out = response.getWriter();

            this.writeResponse(response, respuesta.toString());
            //Verificación de la existencia de un correo
        } else if ((request.getParameter("correo") != null)) {
            StringBuilder respuesta = new StringBuilder("");
            UsuarioDao uDao = new UsuarioDao();

            respuesta.append(faUsu.validarExistenciaCorreo(request.getParameter("correo").trim()));

            PrintWriter out = response.getWriter();

            //out.print(request.getParameter("correo").trim());
            this.writeResponse(response, respuesta.toString());
            
        } else if (request.getParameter("idOferta") != null) {

            FOferta fOfer = new FOferta();
            String cantidad = fOfer.actualizarCantidad(Integer.parseInt(request.getParameter("idOferta")));

            this.writeResponse(response, cantidad);
        } else if (request.getParameter("idOfertaP") != null) {
            FOferta faOfer = new FOferta();
            OfertaDto ofDto = faOfer.obtenerOfertaPorId(Integer.parseInt(request.getParameter("idOfertaP")));
            String promocion = ofDto.getProDto().getDescripcion();
            this.writeResponse(response, promocion);
        } else if (request.getParameter("idOfertaC") != null && request.getParameter("cantidad") != null) {
            int cantidad = 0;
            if (request.getParameter("cantidad").equals("")) {
                cantidad = 0;
            } else {
                cantidad = Integer.parseInt(request.getParameter("cantidad"));
            }
            FOferta faOfer = new FOferta();
            OfertaDto ofDto = faOfer.obtenerOfertaPorId(Integer.parseInt(request.getParameter("idOfertaC")));
            float calculo = ofDto.getPrecioVenta();
            calculo = calculo * cantidad;
            int calculoTotal = (int) calculo;
            String total = Integer.toString(calculoTotal);
            this.writeResponse(response, total);
        }else if (request.getParameter("cantidadPedida") != null && request.getParameter("idOfer") != null && request.getParameter("idPedi") != null) {
            int cantidad = 0;
            if (request.getParameter("cantidadPedida").equals("")) {
                 cantidad = 0;
            }
            else{
                cantidad = Integer.parseInt(request.getParameter("cantidadPedida"));
            }
            FPedido  faPedi = new FPedido();            
               faPedi.actulizarCantidad(cantidad, Integer.parseInt(request.getParameter("idOfer")), Integer.parseInt(request.getParameter("idPedi"))); 
             String salida = faPedi.calcularTotalPedido(Integer.parseInt(request.getParameter("idPedi")));
            this.writeResponse(response, salida);
        }

    }

    public void writeResponse(HttpServletResponse response, String output) throws IOException {
        response.setContentType("text/plain");
        response.setHeader("Cache-Control", "no-cache");
        response.setHeader("Content", "text/html;charset=iso-8859-1");
        response.getWriter().write(output);
    }

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Short description";
    }// </editor-fold>

}
