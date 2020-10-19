package br.com.alura.ecommerce;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();
    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<Email>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        try {

            // we are not caring about any security issues, we are only showing how to use
            // http as a starting point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = UUID.randomUUID().toString();

            var order = new Order(orderId, amount, email);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

            var emailCode = new Email("Ordem@gmail.com", "Bem vindo, estamos processando sua ordem !");
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);

            System.out.println("Processo da nova compra terminado.");

            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Nova ordem enviado");

        } catch (InterruptedException e) {
            throw new ServletException(e);
        } catch (ExecutionException e) {
            throw new ServletException(e);
        }


    }
}
