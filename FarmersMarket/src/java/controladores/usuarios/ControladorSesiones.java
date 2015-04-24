/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controladores.usuarios;

import java.io.IOException;
import java.util.ArrayList;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import modulo.usuarios.FUsuario;
import modulo.usuarios.dto.RolDto;
import modulo.usuarios.dto.UsuarioDto;

/**
 *
 * @author Akashyro
 */
public class ControladorSesiones extends HttpServlet {

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
        
         request.setCharacterEncoding("UTF-8");

        FUsuario faUsu = new FUsuario();

        //Inicio de sesión dentro del sistema
        if ((request.getParameter("botonIniciar") != null) && (request.getParameter("botonIniciar").equals("Ingresar"))) {
            if ((request.getParameter("isDocumento") != null) && (request.getParameter("isClave") != null)) {

                long documento = Long.parseLong(request.getParameter("isDocumento"));
                String clave = request.getParameter("isClave");

                UsuarioDto usuario;

                usuario = faUsu.validarLaSesion(documento);

                if (usuario.getClave().equals("")) {
                    response.sendRedirect("index.jsp?msg=<strong><i class='glyphicon glyphicon-remove'></i> ¡Los datos son invalidos!</strong> &tipoAlert=warning");
                } else if (usuario.getClave().equals(clave)) {
                    HttpSession miSesion = request.getSession();
                    HttpSession rolesUsuario = request.getSession();

                    UsuarioDto queryPersona;
                    queryPersona = faUsu.obtenerUsuarioPorDocumento(usuario.getIdUsuario());
                    miSesion.setAttribute("usuarioEntro", queryPersona);

                    ArrayList<RolDto> rolesActual;
                    rolesActual = faUsu.obtenerRolesPorUsuario(documento);
                    rolesUsuario.setAttribute("roles", rolesActual);

                    boolean paraProductor = false;
                    boolean paraCliente = false;
                    boolean paraAdmin = false;

                    for (RolDto r : rolesActual) {
                        if (r.getIdRol() == 1) {
                            paraProductor = true;
                        } else if (r.getIdRol() == 2) {
                            paraCliente = true;
                        } else if (r.getIdRol() == 3) {
                            paraAdmin = true;
                        }
                    }

                    if (paraProductor) {
                        response.sendRedirect("pages/indexp.jsp");
                    } else if (paraCliente) {
                        response.sendRedirect("pages/indexp.jsp");
                    } else if (paraAdmin) {
                        response.sendRedirect("pages/indexa.jsp");
                    }
                } else {
                    response.sendRedirect("index.jsp?msg=<strong><i class='glyphicon glyphicon-remove'></i> ¡Los datos son invalidos!</strong> Intente nuevamente.&tipoAlert=warning");
                }
            } else {
                response.sendRedirect("index.jsp?msg=<strong><i class='glyphicon glyphicon-remove'></i> ¡CAMPOS VACIOS!</strong> Intente nuevamente.&tipoAlert=warning");
            }
            
            //Cerrar sesión en el sistema.
        } else if (request.getParameter("op").equals("salir")) {
            HttpSession miSesion = request.getSession();
            miSesion.invalidate();

            response.sendRedirect("index.jsp");
        }
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
