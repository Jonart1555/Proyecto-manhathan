import tkinter as tk
from tkinter import messagebox
import random

class Card:
    def __init__(self, name, attack, defense):
        self.name = name
        self.attack = attack
        self.defense = defense

    def __str__(self):
        return f"{self.name} - Ataque: {self.attack}, Defensa: {self.defense}"

class Player:
    def __init__(self, name):
        self.name = name
        self.health = 20  # Cada jugador empieza con 20 de salud
        self.deck = []
        self.hand = []

    def draw_card(self):
        if self.deck:
            card = self.deck.pop()
            self.hand.append(card)
            return card
        return None

    def remove_card(self, card):
        self.hand.remove(card)

class Game:
    def __init__(self, gui):
        self.gui = gui
        self.players = [Player("Jugador 1"), Player("Jugador 2")]
        self.current_turn = 0
        self.setup_game()

    def setup_game(self):
        # Crear un mazo de cartas
        deck = [
            Card("Guerrero", attack=random.randint(3, 10), defense=random.randint(1, 5)),
            Card("Arquero", attack=random.randint(2, 8), defense=random.randint(1, 3)),
            Card("Mago", attack=random.randint(4, 9), defense=random.randint(0, 2)),
        ]
        random.shuffle(deck)

        # Dividir el mazo entre los dos jugadores
        mid = len(deck) // 2
        self.players[0].deck = deck[:mid]
        self.players[1].deck = deck[mid:]

        # Cada jugador roba 2 cartas
        for i in range(2):
            self.players[0].draw_card()
            self.players[1].draw_card()

        # Actualizar interfaz gráfica
        self.gui.update_display()

    def play_turn(self):
        current_player = self.players[self.current_turn]
        opponent_player = self.players[1 - self.current_turn]

        # Verificar si hay cartas en mano
        if current_player.hand:
            card_to_play = current_player.hand[0]  # Escoger la primera carta
            damage = card_to_play.attack
            opponent_player.health -= damage

            # Mensaje del turno
            messagebox.showinfo("Turno", f"{current_player.name} ataca a {opponent_player.name} con {card_to_play.name} causando {damage} de daño.")

            # Actualizar cartas y comprobar victoria
            current_player.remove_card(card_to_play)
            if opponent_player.health <= 0:
                self.gui.update_display()
                messagebox.showinfo("Fin del Juego", f"¡{current_player.name} ha ganado!")
                self.gui.window.quit()
                return

            # Robar nueva carta
            current_player.draw_card()

        else:
            messagebox.showinfo("Sin Cartas", f"{current_player.name} no tiene cartas para jugar.")

        # Cambiar turno
        self.current_turn = 1 - self.current_turn
        self.gui.update_display()

class GameGUI:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("Juego de Cartas")
        self.game = Game(self)

        # Labels para mostrar información de jugadores
        self.player1_info = tk.Label(self.window, text="", font=("Arial", 12))
        self.player1_info.pack()

        self.player2_info = tk.Label(self.window, text="", font=("Arial", 12))
        self.player2_info.pack()

        # Botón para jugar turno
        self.play_button = tk.Button(self.window, text="Jugar Turno", command=self.game.play_turn)
        self.play_button.pack()

        # Mostrar cartas en mano
        self.hand_display = tk.Label(self.window, text="", font=("Arial", 12))
        self.hand_display.pack()

    def update_display(self):
        player1 = self.game.players[0]
        player2 = self.game.players[1]

        # Actualizar la información de los jugadores
        self.player1_info.config(text=f"{player1.name} - Salud: {player1.health}, Cartas en Mano: {len(player1.hand)}")
        self.player2_info.config(text=f"{player2.name} - Salud: {player2.health}, Cartas en Mano: {len(player2.hand)}")

        # Mostrar las cartas en la mano del jugador actual
        current_player = self.game.players[self.game.current_turn]
        self.hand_display.config(text="Cartas en mano: " + ', '.join(str(card) for card in current_player.hand))

    def run(self):
        self.update_display()
        self.window.mainloop()

# Ejecución del juego con interfaz gráfica
if __name__ == "__main__":
    gui = GameGUI()
    gui.run()